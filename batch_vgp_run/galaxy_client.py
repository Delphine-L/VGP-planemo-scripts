#!/usr/bin/env python3

"""
Galaxy API client functions for VGP pipeline.

This module provides functions for interacting with Galaxy instances:
- Dataset and invocation management
- Status checking and polling
- History and invocation searching
- Report downloading
"""

import os
import json
import time
from datetime import datetime
from batch_vgp_run.logging_utils import log_info, log_warning


def get_datasets_ids_from_json(json_path):
    """
    Extract dataset IDs from a planemo invocation JSON file.
    When planemo runs without --no-wait, the JSON contains all output information.

    Args:
        json_path (str): Path to the planemo invocation JSON file

    Returns:
        dict: Dataset IDs mapped by output label, or None if file doesn't exist or workflow incomplete
    """
    if not os.path.exists(json_path):
        return None

    try:
        with open(json_path, 'r') as f:
            data = json.load(f)

        # Get invocation details from planemo JSON structure
        test_data = data.get('tests', [{}])[0].get('data', {})

        # Check if workflow completed successfully
        status = test_data.get('status')
        if status not in ['success', 'error']:  # error state might still have outputs
            return None

        # Get the invocation details
        invocation_details = test_data.get('invocation_details', {}).get('details', {})

        if not invocation_details:
            return None

        # Try to read from the invocation details embedded in the test output
        # Planemo should have populated this when workflow completed
        if 'invocation' in test_data:
            invocation = test_data['invocation']
            return get_datasets_ids(invocation)

        # Fallback: return None if invocation data not in JSON
        return None

    except (json.JSONDecodeError, KeyError, IndexError) as e:
        print(f"Warning: Could not parse dataset IDs from {json_path}: {e}")
        return None


def get_datasets_ids(invocation):
    """
    Extract dataset IDs from a Galaxy invocation object.

    Args:
        invocation (dict): Galaxy invocation object

    Returns:
        dict: Dataset IDs mapped by output/input label
    """
    dic_datasets_ids = {key: value['id'] for key, value in invocation['outputs'].items()}
    dic_datasets_ids.update({value['label']: value['id'] for key, value in invocation['inputs'].items()})
    dic_datasets_ids.update({value['label']: value['parameter_value'] for key, value in invocation['input_step_parameters'].items()})
    dic_datasets_ids.update({key: value['id'] for key, value in invocation['output_collections'].items()})
    return dic_datasets_ids


def check_invocation_complete(gi, invocation_id):
    """
    Check if a workflow invocation is complete.

    Args:
        gi (GalaxyInstance): Galaxy instance object
        invocation_id (str): Invocation ID to check

    Returns:
        tuple: (is_complete, status) where is_complete is bool and status is the state string
               States: 'new', 'ready', 'scheduled', 'ok', 'error', 'failed', 'cancelled'
               Complete = 'ok' or 'scheduled' (all jobs queued/running/done)
    """
    try:
        summary = gi.invocations.get_invocation_summary(str(invocation_id))
        status = summary.get('populated_state', 'unknown')
        # scheduled means all jobs are queued/running, ok means all complete successfully
        is_complete = status in ['ok', 'scheduled']
        return (is_complete, status)
    except Exception as e:
        print(f"  Warning: Could not check status for invocation {invocation_id}: {e}")
        return (False, 'error')


def check_mitohifi_failure(gi, invocation_id):
    """
    Check if a Workflow 0 (mitochondrial) failure is due to no mitochondrial reads.

    Args:
        gi (GalaxyInstance): Galaxy instance object
        invocation_id (str): Invocation ID to check

    Returns:
        tuple: (is_no_mito_data: bool, error_message: str)
               is_no_mito_data: True if failure is due to no mitochondrial reads
               error_message: Descriptive error message
    """
    try:
        invocation = gi.invocations.show_invocation(str(invocation_id))

        # Find the MitoHifi step in the workflow
        steps = invocation.get('steps', [])
        for step in steps:
            step_jobs = step.get('jobs', [])
            for job in step_jobs:
                # Check if this is a MitoHifi step (look for tool name or step name)
                tool_id = job.get('tool_id', '')
                if 'mitohifi' in tool_id.lower():
                    job_id = job.get('id')
                    if job_id:
                        # Get detailed job information including stdout/stderr
                        try:
                            job_details = gi.jobs.show_job(job_id, full_details=True)

                            # Check stdout for "Total number of mapped reads: 0"
                            stdout = job_details.get('stdout', '')
                            stderr = job_details.get('stderr', '')

                            no_mapped_reads = 'Total number of mapped reads: 0' in stdout
                            hifiasm_error = 'An error may have occurred when assembling reads with HiFiasm.' in stderr

                            if no_mapped_reads and hifiasm_error:
                                return (True, "Reads probably contain no mitochondrial data (MitoHifi found 0 mapped reads)")
                            elif no_mapped_reads:
                                return (True, "No mitochondrial reads found (MitoHifi mapped 0 reads)")

                        except Exception as e:
                            log_warning(f"Could not retrieve job details for MitoHifi step: {e}")

        # MitoHifi step found but no specific error pattern
        return (False, "Invocation in error (unknown MitoHifi issue)")

    except Exception as e:
        log_warning(f"Could not check MitoHifi failure pattern: {e}")
        return (False, f"Invocation in error (could not diagnose: {e})")


def check_required_outputs_exist(gi, invocation_id, required_outputs):
    """
    Check if specific required outputs exist in a workflow invocation.
    This allows launching downstream workflows before the upstream workflow is fully complete.

    Args:
        gi (GalaxyInstance): Galaxy instance object
        invocation_id (str): Invocation ID to check
        required_outputs (list): List of output names that must exist (e.g., ['gfa_assembly', 'Estimated Genome size'])

    Returns:
        tuple: (outputs_ready, missing_outputs)
               outputs_ready (bool): True if all required outputs exist
               missing_outputs (list): List of output names that are missing
    """
    try:
        # Get full invocation object with outputs
        invocation = gi.invocations.show_invocation(str(invocation_id))

        # Extract all dataset IDs/outputs using existing function
        available_outputs = get_datasets_ids(invocation)

        # Check which required outputs are missing
        missing_outputs = [output for output in required_outputs if output not in available_outputs]

        outputs_ready = len(missing_outputs) == 0

        return (outputs_ready, missing_outputs)
    except Exception as e:
        print(f"  Warning: Could not check outputs for invocation {invocation_id}: {e}")
        return (False, required_outputs)  # Assume all outputs missing on error


def get_or_find_history_id(gi, list_metadata, assembly_id, invocation_id=None, is_resume=False):
    """
    Get history_id from metadata, from an invocation, or search for it.
    Stores found history_id in metadata for reuse across all workflow searches.

    Priority:
    1. Return cached history_id if exists in metadata (works for both new and resume runs)
    2. Get from invocation_id if provided (requires API call)
    3. Search by history name (only during resume, may not be accurate if duplicates exist)

    Args:
        gi (GalaxyInstance): Galaxy instance object
        list_metadata (dict): Metadata dictionary
        assembly_id (str): Assembly ID
        invocation_id (str, optional): Invocation ID to get history from
        is_resume (bool): Whether this is a resume run (enables history search)

    Returns:
        str: history_id or None if not found
    """
    # Check cached first (works for both new and resume runs - avoids API calls)
    if 'history_id' in list_metadata[assembly_id] and list_metadata[assembly_id]['history_id'] != 'NA':
        return list_metadata[assembly_id]['history_id']

    # Get from invocation if provided (requires API call - only if not cached)
    if invocation_id:
        try:
            invocation_details = gi.invocations.show_invocation(str(invocation_id))
            history_id = invocation_details['history_id']
            # Cache it in metadata
            list_metadata[assembly_id]['history_id'] = history_id
            print(f"Retrieved history_id {history_id} from invocation {invocation_id}")
            return history_id
        except Exception as e:
            print(f"Warning: Could not get history from invocation {invocation_id}: {e}")

    # For new runs (not resume), don't search for history - it will be created by WF1
    if not is_resume:
        return None

    # Fallback: Search by history name (only during resume, may not be accurate if duplicates)
    history_name = list_metadata[assembly_id]['History_name']
    try:
        history_list = gi.histories._get_histories(name=history_name)
        if len(history_list) > 1:
            print(f"Warning: Multiple histories found for '{history_name}'. Using most recent (may not be correct).")
            hist_times = {hist['id']: hist['update_time'] for hist in history_list}
            sorted_hists = sorted(hist_times.items(), key=lambda item: item[1], reverse=True)
            history_id = sorted_hists[0][0]
        elif len(history_list) == 1:
            history_id = history_list[0]['id']
        else:
            return None

        # Cache it in metadata
        list_metadata[assembly_id]['history_id'] = history_id
        return history_id
    except Exception as e:
        print(f"Warning: Could not search for history '{history_name}': {e}")
        return None


def build_invocation_cache(gi, history_id):
    """
    Build a cache of all invocations in a history.
    This is called once per species to avoid repeated API calls.

    Args:
        gi (GalaxyInstance): Galaxy instance object
        history_id (str): History ID

    Returns:
        dict: Cache with structure:
            {
                'invocations': {inv_id: inv_details, ...},
                'workflows': {wf_id: wf_name, ...},
                'subworkflows': set([inv_id, ...])
            }
    """
    cache = {
        'invocations': {},
        'workflows': {},
        'subworkflows': set()
    }

    try:
        # Get all invocations for this history (1 API call)
        invocations = gi.invocations.get_invocations(history_id=history_id)

        for invoc in invocations:
            inv_id = invoc['id']
            try:
                # Get full invocation details (1 API call per invocation)
                inv_details = gi.invocations.show_invocation(inv_id)
                cache['invocations'][inv_id] = inv_details

                # Collect subworkflow IDs
                for step in inv_details.get('steps', []):
                    if step.get('subworkflow_invocation_id'):
                        cache['subworkflows'].add(step['subworkflow_invocation_id'])

                # Get workflow name only once per unique workflow_id
                wf_id = inv_details.get('workflow_id', '')
                if wf_id and wf_id not in cache['workflows']:
                    try:
                        wf = gi.workflows.show_workflow(workflow_id=wf_id, instance=True)
                        cache['workflows'][wf_id] = wf['name']
                    except:
                        cache['workflows'][wf_id] = ''
            except:
                continue

        print(f"Built invocation cache: {len(cache['invocations'])} invocations, {len(cache['workflows'])} workflows")
        return cache

    except Exception as e:
        print(f"Warning: Could not build invocation cache: {e}")
        return cache


def fetch_invocation_from_history(gi, history_id, workflow_name, haplotype=None, cache=None):
    """
    Fetch invocation ID from Galaxy history when JSON file is missing.
    Returns None if not found (this is OK - workflow will be launched).

    Args:
        gi (GalaxyInstance): Galaxy instance object
        history_id (str): History ID (already looked up and cached)
        workflow_name (str): Workflow name to match (e.g., "VGP1", "VGP4", "VGP8")
        haplotype (str, optional): For VGP8/VGP9, specify 'hap1' or 'hap2'
        cache (dict, optional): Pre-built invocation cache from build_invocation_cache()

    Returns:
        str: invocation_id or None if not found
    """
    try:
        # Use provided cache or build a new one (for backward compatibility)
        if cache is None:
            cache = build_invocation_cache(gi, history_id)

        # Process cached invocations - no additional API calls!
        matching_invocations = {}

        for inv_id, inv_details in cache['invocations'].items():
            # Skip subworkflows
            if inv_id in cache['subworkflows']:
                continue

            # Get workflow name from cache
            wf_id = inv_details.get('workflow_id', '')
            wf_name = cache['workflows'].get(wf_id, '')

            # Match workflow name
            if workflow_name in wf_name:
                # Check state (already in inv_details)
                state = inv_details.get('state', '')
                if state not in ['failed', 'cancelled']:
                    # For VGP8/VGP9, check haplotype parameter
                    if haplotype:
                        if 'Haplotype' in inv_details.get('input_step_parameters', {}):
                            invoc_hap = inv_details['input_step_parameters']['Haplotype']['parameter_value']
                            invoc_hap = invoc_hap.replace('Haplotype ', 'hap')
                            if invoc_hap == haplotype:
                                matching_invocations[inv_id] = inv_details['create_time']
                    else:
                        matching_invocations[inv_id] = inv_details['create_time']

        # Return most recent matching invocation
        if matching_invocations:
            sorted_invocs = sorted(matching_invocations.items(), key=lambda item: item[1], reverse=True)
            return sorted_invocs[0][0]

        return None
    except Exception as e:
        print(f"Warning: Error searching history for {workflow_name}: {e}")
        return None


def poll_until_invocation_complete(gi, invocation_id, workflow_name, assembly_id, poll_interval=3600, max_polls=24):
    """
    Poll an invocation until it reaches a terminal state (ok, error, failed, cancelled).
    Used in resume mode when an invocation is still running.

    Args:
        gi (GalaxyInstance): Galaxy instance object
        invocation_id (str): Invocation ID to poll
        workflow_name (str): Workflow name for logging
        assembly_id (str): Assembly ID for logging
        poll_interval (int): Seconds between polls (default: 3600 = 1 hour)
        max_polls (int): Maximum number of polls before giving up (default: 24 = 24 hours)

    Returns:
        tuple: (is_complete: bool, state: str)
    """
    print(f"\n{'='*60}")
    print(f"Polling {workflow_name} for {assembly_id} (invocation: {invocation_id})")
    print(f"Status will be checked every {poll_interval//60} minutes")
    print(f"{'='*60}\n")

    for poll_count in range(max_polls):
        try:
            # Get current timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            summary = gi.invocations.get_invocation_summary(str(invocation_id))
            state = summary.get('populated_state', 'unknown')

            # Extract job counts from summary
            jobs = summary.get('jobs', [])
            total_jobs = len(jobs)

            # Count jobs by state
            job_states = {}
            for job in jobs:
                job_state = job.get('state', 'unknown')
                job_states[job_state] = job_states.get(job_state, 0) + 1

            completed_jobs = job_states.get('ok', 0)
            failed_jobs = job_states.get('error', 0) + job_states.get('failed', 0)
            running_jobs = job_states.get('running', 0)
            queued_jobs = job_states.get('queued', 0) + job_states.get('new', 0)

            # Build progress string
            progress_parts = []
            if completed_jobs > 0:
                progress_parts.append(f"{completed_jobs} completed")
            if running_jobs > 0:
                progress_parts.append(f"{running_jobs} running")
            if queued_jobs > 0:
                progress_parts.append(f"{queued_jobs} queued")
            if failed_jobs > 0:
                progress_parts.append(f"{failed_jobs} failed")

            progress_str = ", ".join(progress_parts) if progress_parts else "no jobs"

            print(f"Poll {poll_count + 1}/{max_polls} [{timestamp}]: {workflow_name} state = {state}")
            print(f"  Jobs: {completed_jobs}/{total_jobs} ({progress_str})")

            # Check if reached terminal state
            if state in ['ok', 'error', 'failed', 'cancelled']:
                if state == 'ok':
                    print(f"✓ {workflow_name} completed successfully!")
                    return (True, state)
                else:
                    print(f"✗ {workflow_name} finished with state: {state}")
                    return (True, state)

            # Still running, wait before next poll
            if poll_count < max_polls - 1:  # Don't sleep on last iteration
                print(f"  Workflow still running. Sleeping for {poll_interval//60} minutes...")
                print(f"  (You can safely interrupt with Ctrl+C and resume later)\n")
                time.sleep(poll_interval)

        except Exception as e:
            print(f"Warning: Error checking invocation status: {e}")
            if poll_count < max_polls - 1:
                print(f"  Retrying in {poll_interval//60} minutes...\n")
                time.sleep(poll_interval)

    # Max polls reached
    print(f"⚠ Maximum polling time reached for {workflow_name}")
    return (False, 'timeout')


def poll_until_outputs_ready(gi, invocation_id, required_outputs, workflow_name, assembly_id, poll_interval=3600, max_polls=72):
    """
    Poll an invocation until required outputs are ready (exist in the invocation).
    Used when we need specific outputs before launching the next workflow.

    Args:
        gi (GalaxyInstance): Galaxy instance object
        invocation_id (str): Invocation ID to check
        required_outputs (list): List of required output names
        workflow_name (str): Workflow name for logging
        assembly_id (str): Assembly ID for logging
        poll_interval (int): Seconds between polls (default: 3600 = 1 hour)
        max_polls (int): Maximum number of polls before giving up (default: 72 = 3 days)

    Returns:
        tuple: (outputs_ready: bool, missing_outputs: list)
    """
    print(f"\n{'='*60}")
    print(f"Polling {workflow_name} outputs for {assembly_id} (invocation: {invocation_id})")
    print(f"Waiting for outputs: {', '.join(required_outputs)}")
    print(f"Status will be checked every {poll_interval//60} minutes")
    print(f"{'='*60}\n")

    for poll_count in range(max_polls):
        try:
            # Get current timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Check if required outputs exist
            outputs_ready, missing_outputs = check_required_outputs_exist(gi, invocation_id, required_outputs)

            if outputs_ready:
                print(f"✓ All required outputs are ready for {workflow_name}!")
                return (True, [])
            else:
                print(f"Poll {poll_count + 1}/{max_polls} [{timestamp}]: Still missing outputs: {', '.join(missing_outputs)}")

            # Check invocation state and job progress for context
            try:
                summary = gi.invocations.get_invocation_summary(str(invocation_id))
                state = summary.get('populated_state', 'unknown')

                # Extract job counts from summary
                jobs = summary.get('jobs', [])
                total_jobs = len(jobs)

                # Count jobs by state
                job_states = {}
                for job in jobs:
                    job_state = job.get('state', 'unknown')
                    job_states[job_state] = job_states.get(job_state, 0) + 1

                completed_jobs = job_states.get('ok', 0)
                failed_jobs = job_states.get('error', 0) + job_states.get('failed', 0)
                running_jobs = job_states.get('running', 0)
                queued_jobs = job_states.get('queued', 0) + job_states.get('new', 0)

                # Build progress string
                progress_parts = []
                if completed_jobs > 0:
                    progress_parts.append(f"{completed_jobs} completed")
                if running_jobs > 0:
                    progress_parts.append(f"{running_jobs} running")
                if queued_jobs > 0:
                    progress_parts.append(f"{queued_jobs} queued")
                if failed_jobs > 0:
                    progress_parts.append(f"{failed_jobs} failed")

                progress_str = ", ".join(progress_parts) if progress_parts else "no jobs"
                print(f"  Invocation state: {state} | Jobs: {completed_jobs}/{total_jobs} ({progress_str})")

                # If invocation failed, no point in waiting
                if state in ['failed', 'cancelled', 'error']:
                    print(f"✗ {workflow_name} invocation failed with state: {state}")
                    print(f"  Required outputs will not be generated.")
                    return (False, missing_outputs)
            except Exception as e:
                print(f"  Warning: Could not check invocation state: {e}")

            # Still waiting, sleep before next poll
            if poll_count < max_polls - 1:  # Don't sleep on last iteration
                print(f"  Sleeping for {poll_interval//60} minutes...")
                print(f"  (You can safely interrupt with Ctrl+C and resume later)\n")
                time.sleep(poll_interval)

        except Exception as e:
            print(f"Warning: Error checking outputs: {e}")
            if poll_count < max_polls - 1:
                print(f"  Retrying in {poll_interval//60} minutes...\n")
                time.sleep(poll_interval)

    # Max polls reached
    print(f"⚠ Maximum polling time reached for {workflow_name} outputs")
    print(f"  Still missing: {', '.join(missing_outputs)}")
    return (False, missing_outputs)


def download_invocation_report(gi, invocation_id, output_path, workflow_name="workflow", assembly_id="unknown"):
    """
    Download PDF report for a completed workflow invocation.
    This feature can be unreliable, so errors are caught and logged but don't stop execution.

    Args:
        gi (GalaxyInstance): Galaxy instance object
        invocation_id (str): Invocation ID
        output_path (str): Path where to save the PDF report
        workflow_name (str): Workflow name for logging
        assembly_id (str): Assembly ID for logging

    Returns:
        bool: True if download successful, False otherwise
    """
    try:
        # Check if invocation is in a terminal state
        invocation = gi.invocations.show_invocation(str(invocation_id))
        state = invocation.get('state', 'unknown')

        if state not in ['ok', 'failed', 'cancelled']:
            log_warning(f"Cannot download report for {workflow_name} ({assembly_id}): invocation not in terminal state (state: {state})")
            return False

        # Get the report PDF
        log_info(f"Downloading report for {workflow_name} ({assembly_id})...")
        report_pdf = gi.invocations.show_invocation_report_pdf(str(invocation_id))

        # Write to file
        with open(output_path, 'wb') as pdf_file:
            pdf_file.write(report_pdf)

        log_info(f"✓ Report saved: {output_path}")
        return True

    except Exception as e:
        # This feature is unreliable, so just log warning and continue
        log_warning(f"Could not download report for {workflow_name} ({assembly_id}): {e}")
        return False


def batch_update_metadata_from_histories(gi, list_metadata, profile_data, suffix_run, download_reports=False):
    """
    Pre-populate all species metadata with invocations from their histories.
    This centralizes all API calls before threading to minimize API load.

    Args:
        gi (GalaxyInstance): Galaxy instance object
        list_metadata (dict): All species metadata
        profile_data (dict): Profile configuration
        suffix_run (str): Run suffix
        download_reports (bool): Whether to download PDF reports

    Returns:
        None (updates list_metadata in place)
    """
    # Import here to avoid circular dependency
    from batch_vgp_run.metadata import save_species_metadata

    print(f"\n{'='*60}")
    print("Pre-fetching invocations from Galaxy histories...")
    print(f"{'='*60}\n")

    # Group species by history_id to minimize API calls
    history_to_species = {}
    for species_id, metadata in list_metadata.items():
        history_id = metadata.get('history_id', 'NA')
        if history_id != 'NA':
            if history_id not in history_to_species:
                history_to_species[history_id] = []
            history_to_species[history_id].append(species_id)

    if not history_to_species:
        print("No histories to fetch from.\n")
        return

    print(f"Found {len(history_to_species)} unique histories to check")

    # Workflow keys to search for
    workflow_search_map = {
        'Workflow_1': 'VGP1',
        'Workflow_4': 'VGP4',
        'Workflow_0': 'VGP0',
        'Workflow_8_hap1': ('VGP8', 'hap1'),
        'Workflow_8_hap2': ('VGP8', 'hap2'),
        'Workflow_9_hap1': ('VGP9', 'hap1'),
        'Workflow_9_hap2': ('VGP9', 'hap2'),
        'Workflow_PreCuration': 'PretextMap'
    }

    # Process each unique history
    for history_id, species_ids in history_to_species.items():
        print(f"\nFetching invocations for history: {history_id}")
        print(f"  Species in this history: {', '.join(species_ids)}")

        # Build cache once for this history (batch API call)
        cache = build_invocation_cache(gi, history_id)

        # Update all species that share this history
        for species_id in species_ids:
            print(f"\n  Updating {species_id}...")
            updated_count = 0

            for workflow_key, search_params in workflow_search_map.items():
                # Skip if already have invocation
                current_inv = list_metadata[species_id]['invocations'].get(workflow_key, 'NA')
                if current_inv != 'NA':
                    continue

                # Search for invocation
                if isinstance(search_params, tuple):
                    wf_name, haplotype = search_params
                    invocation_id = fetch_invocation_from_history(gi, history_id, wf_name, haplotype=haplotype, cache=cache)
                else:
                    invocation_id = fetch_invocation_from_history(gi, history_id, search_params, cache=cache)

                if invocation_id:
                    # Check if this invocation is in a failed state
                    # (Important for --retry-failed: don't re-add failed invocations)
                    inv_details = cache['invocations'].get(invocation_id)
                    if inv_details:
                        inv_state = inv_details.get('state', 'unknown')
                        if inv_state in ['failed', 'cancelled', 'error']:
                            # Don't re-add failed invocations - leave as 'NA' so they can be retried
                            print(f"    Skipping {workflow_key}: invocation {invocation_id} is in '{inv_state}' state")
                            continue

                    list_metadata[species_id]['invocations'][workflow_key] = invocation_id
                    print(f"    Found {workflow_key}: {invocation_id}")

                    # Get dataset IDs for this invocation
                    try:
                        # Use cached invocation details (no new API call!)
                        if inv_details:
                            dataset_ids = get_datasets_ids(inv_details)
                            list_metadata[species_id]['dataset_ids'][workflow_key] = dataset_ids
                            print(f"      Retrieved {len(dataset_ids)} dataset IDs")
                        else:
                            # Fallback: make API call if not in cache
                            inv_details = gi.invocations.show_invocation(str(invocation_id))
                            dataset_ids = get_datasets_ids(inv_details)
                            list_metadata[species_id]['dataset_ids'][workflow_key] = dataset_ids
                            print(f"      Retrieved {len(dataset_ids)} dataset IDs (fallback)")
                    except Exception as e:
                        print(f"      Warning: Could not get dataset IDs: {e}")

                    # Download report if requested
                    if download_reports:
                        report_path = list_metadata[species_id]['reports'].get(workflow_key)
                        if report_path:
                            download_invocation_report(gi, invocation_id, report_path, workflow_key, species_id)

                    updated_count += 1

            if updated_count > 0:
                # Save updated metadata for this species
                save_species_metadata(species_id, list_metadata, profile_data, suffix_run)
                print(f"  ✓ Updated {updated_count} workflows for {species_id}")
            else:
                print(f"  No new invocations found for {species_id}")

    print(f"\n{'='*60}")
    print("Finished pre-fetching invocations")
    print(f"{'='*60}\n")
