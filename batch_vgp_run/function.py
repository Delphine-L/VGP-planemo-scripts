#!/usr/bin/env python3


import os
import sys
import requests
import zipfile
import json
import shutil
from bioblend.galaxy import GalaxyInstance
import re
from collections import defaultdict
import time
from datetime import datetime
import threading
import subprocess
import logging

# Set up logging
logger = logging.getLogger(__name__)

def setup_logging(quiet=False):
    """
    Configure logging for the VGP pipeline.

    Args:
        quiet (bool): If True, only show warnings and errors
    """
    # Set logging level based on quiet flag
    level = logging.WARNING if quiet else logging.INFO

    # Configure logging format
    log_format = '%(message)s'  # Simple format for user-facing messages

    # Configure root logger
    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Set module logger level
    logger.setLevel(level)

def log_info(message):
    """Log informational message (suppressed in quiet mode)."""
    logger.info(message)

def log_warning(message):
    """Log warning message to both logger and stderr (always shown)."""
    logger.warning(message)
    print(f"Warning: {message}", file=sys.stderr)

def log_error(message):
    """Log error message to both logger and stderr (always shown)."""
    logger.error(message)
    print(f"Error: {message}", file=sys.stderr)

def find_duplicate_values(input_dict):
    """
    Finds values in a dictionary that are associated with multiple keys.

    Args:
        input_dict (dict): The dictionary to search for duplicate values.

    Returns:
        dict: A dictionary where keys are the duplicate values and values are 
              lists of keys that share that value.
    """
    reversed_dict = defaultdict(list)
    for key, value in input_dict.items():
        reversed_dict[value].append(key)

    duplicate_values = {
        value: keys 
        for value, keys in reversed_dict.items() 
        if len(keys) > 1
    }
    return duplicate_values


def download_file(url, save_path):
    """
    Downloads a file from a given URL and saves it to a specified path.

    Args:
        url (str): The URL of the file to download.
        save_path (str): The local path to save the downloaded file.
    """
    try:
        response = requests.get(url, stream=True)  # Use stream=True for large files
        response.raise_for_status()  # Raise an exception for bad status codes

        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"File downloaded successfully to: {save_path}")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def get_workflow_version(path_ga):
    release_line=''
    try:
        wfjson=open(path_ga)
        gawf=json.load(wfjson)
        if 'release' in gawf.keys():
            release_number=gawf['release']
        else:
            release_number='NA'
        return release_number
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")


def get_worfklow(Compatible_version, workflow_name, workflow_repo):
    os.makedirs(workflow_repo, exist_ok=True)
    url_workflow="https://github.com/iwc-workflows/"+workflow_name+"/archive/refs/tags/v"+Compatible_version+".zip"
    path_compatible=workflow_name+"-"+Compatible_version+"/"+workflow_name+".ga"
    file_path = workflow_repo+workflow_name+".ga"
    archive_path=workflow_repo+workflow_name+".zip"
    if os.path.exists(file_path):
        print('Workflow '+workflow_name+" found.\n")
        release_number=get_workflow_version(file_path)
    else:
        release_number=Compatible_version
        download_file(url_workflow, archive_path)
        with zipfile.ZipFile(archive_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            for item in file_list:
                if workflow_name.lower()+".ga" in item.lower():
                    extracted_path=item
                    zip_ref.extract(path=workflow_repo, member=item) 
            os.remove(archive_path)
            shutil.move(workflow_repo+extracted_path, file_path)
            with open(file_path, 'r+') as wf_file:
                workflow_data = json.load(wf_file)
                workflow_data['name'] = f"{workflow_data.get('name', workflow_name)} - v{release_number}"
                wf_file.seek(0)
                json.dump(workflow_data, wf_file, indent=4)
                wf_file.truncate()
        os.rmdir(workflow_repo+workflow_name+"-"+Compatible_version+"/")
    return file_path, release_number


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

        # Extract invocation data from the details (similar structure to API response)
        # The invocation details should have been populated by planemo after workflow completion
        invocation_json_path = json_path.replace('.json', '_invocation.json')

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
    dic_datasets_ids={key: value['id'] for key,value in invocation['outputs'].items()}
    dic_datasets_ids.update({value['label']: value['id'] for key,value in invocation['inputs'].items()})
    dic_datasets_ids.update({value['label']: value['parameter_value'] for key,value in invocation['input_step_parameters'].items()})
    dic_datasets_ids.update({key: value['id'] for key, value in invocation['output_collections'].items()})
    return dic_datasets_ids


def fix_parameters(entered_suffix, entered_url):
    if entered_suffix!='':
        suffix_run='_'+entered_suffix
    else:
        suffix_run=''
    regexurl=r'(https?:\/\/)'
    if re.search(regexurl,entered_url):
        validurl=entered_url
    else:
        validurl='https://'+entered_url
    return suffix_run,validurl



def fix_directory(entered_directory):
    if entered_directory[-1]=="/":
        wfl_dir=entered_directory
    else:
        wfl_dir=entered_directory+"/"
    return wfl_dir

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

def batch_update_metadata_from_histories(gi, list_metadata, profile_data, suffix_run):
    """
    Pre-populate all species metadata with invocations from their histories.
    This centralizes all API calls before threading to minimize API load.

    Args:
        gi (GalaxyInstance): Galaxy instance object
        list_metadata (dict): All species metadata
        profile_data (dict): Profile configuration
        suffix_run (str): Run suffix

    Returns:
        None (updates list_metadata in place)
    """
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
                    list_metadata[species_id]['invocations'][workflow_key] = invocation_id
                    print(f"    Found {workflow_key}: {invocation_id}")

                    # Get dataset IDs for this invocation
                    try:
                        # Use cached invocation details (no new API call!)
                        inv_details = cache['invocations'].get(invocation_id)
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

def wait_for_invocations(gi, invocation_ids, assembly_id, workflow_name=None, poll_interval=60, timeout=86400):
    """
    Wait for Galaxy invocations to complete, polling at regular intervals.

    Args:
        gi (GalaxyInstance): Galaxy instance object
        invocation_ids (list): List of invocation IDs to wait for (or dict {inv_id: description})
        assembly_id (str): Assembly ID for logging
        workflow_name (str, optional): Workflow name for logging
        poll_interval (int): Seconds between status checks (default: 60)
        timeout (int): Maximum seconds to wait (default: 86400 = 24 hours)

    Returns:
        dict: Status of each invocation {invocation_id: status}
    """
    start_time = time.time()

    # Support both list and dict formats
    if isinstance(invocation_ids, dict):
        inv_dict = invocation_ids
        invocation_ids = list(inv_dict.keys())
    else:
        inv_dict = {inv_id: workflow_name or f"invocation {inv_id}" for inv_id in invocation_ids}

    statuses = {inv_id: 'waiting' for inv_id in invocation_ids}

    wf_info = f" ({workflow_name})" if workflow_name else ""
    # Format poll interval in human-readable form
    if poll_interval >= 60:
        poll_display = f"{poll_interval // 60} minutes"
    else:
        poll_display = f"{poll_interval} seconds"

    log_info(f"Waiting for {len(invocation_ids)} invocation(s) to complete for {assembly_id}{wf_info}...")
    log_info(f"Polling every {poll_display}. Press Ctrl+C to interrupt and resume later with --resume.\n")

    poll_count = 0
    while True:
        poll_count += 1
        elapsed = int(time.time() - start_time)
        # Get current timestamp
        from datetime import datetime
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_info(f"[Poll #{poll_count}, {current_time}, elapsed: {elapsed//60}m {elapsed%60}s] Checking invocation status...")

        all_done = True
        for inv_id in invocation_ids:
            if statuses[inv_id] not in ['ok', 'error', 'failed']:
                try:
                    summary = gi.invocations.get_invocation_summary(str(inv_id))
                    status = summary.get('populated_state', 'unknown')

                    # Get job completion statistics
                    jobs_total = 0
                    jobs_complete = 0
                    jobs_ok = 0
                    jobs_error = 0
                    if 'states' in summary:
                        states = summary['states']
                        # Total jobs = sum of all job states
                        jobs_total = sum(states.values()) if states else 0
                        # Completed jobs = ok + error (terminal states)
                        jobs_ok = states.get('ok', 0)
                        jobs_error = states.get('error', 0)
                        jobs_complete = jobs_ok + jobs_error

                    job_info = f" ({jobs_complete}/{jobs_total} jobs)" if jobs_total > 0 else ""

                    # Add warning for failed jobs
                    if jobs_error > 0:
                        job_info += f" ⚠ {jobs_error} failed"

                    # Only print if status changed
                    if status != statuses[inv_id]:
                        statuses[inv_id] = status
                        inv_label = inv_dict.get(inv_id, inv_id)
                        if status == 'ok':
                            log_info(f"  ✓ {inv_label} ({assembly_id}): completed successfully{job_info}")
                        elif status in ['error', 'failed']:
                            log_warning(f"{inv_label} ({assembly_id}): {status}{job_info}")
                        else:
                            log_info(f"  → {inv_label} ({assembly_id}): {status}{job_info}")

                    if status not in ['ok', 'error', 'failed']:
                        all_done = False
                except Exception as e:
                    inv_label = inv_dict.get(inv_id, inv_id)
                    print(f"  Warning: Could not check status for {inv_label}: {e}")
                    all_done = False

        if all_done:
            log_info(f"\n✓ All invocations completed for {assembly_id}{wf_info}\n")
            break

        # Check timeout
        if time.time() - start_time > timeout:
            log_warning(f"Timeout reached after {timeout} seconds ({timeout//3600} hours)")
            break

        # Wait before next check
        log_info(f"  Waiting {poll_display} before next check...")
        time.sleep(poll_interval)

    return statuses

def mark_invocation_as_failed(assembly_id, list_metadata, workflow_key, invocation_id, profile_data, suffix_run):
    """
    Mark an invocation as failed by moving it from invocations to failed_invocations.

    Args:
        assembly_id (str): Assembly ID
        list_metadata (dict): Metadata dictionary
        workflow_key (str): Workflow key (e.g., "Workflow_1", "Workflow_8_hap1")
        invocation_id (str): Invocation ID that failed
        profile_data (dict): Profile configuration
        suffix_run (str): Suffix for the run
    """
    # Initialize failed_invocations dict if it doesn't exist
    if 'failed_invocations' not in list_metadata[assembly_id]:
        list_metadata[assembly_id]['failed_invocations'] = {}

    # Initialize workflow list in failed_invocations if it doesn't exist
    if workflow_key not in list_metadata[assembly_id]['failed_invocations']:
        list_metadata[assembly_id]['failed_invocations'][workflow_key] = []

    # Add to failed list if not already there
    if invocation_id not in list_metadata[assembly_id]['failed_invocations'][workflow_key]:
        list_metadata[assembly_id]['failed_invocations'][workflow_key].append(invocation_id)
        log_warning(f"Invocation {invocation_id} for {workflow_key} marked as failed")

    # Remove from regular invocations (reset to 'NA' so it can be retried)
    if workflow_key in list_metadata[assembly_id]['invocations']:
        list_metadata[assembly_id]['invocations'][workflow_key] = 'NA'

    # Save metadata
    save_species_metadata(assembly_id, list_metadata, profile_data, suffix_run)

def save_species_metadata(assembly_id, list_metadata, profile_data, suffix_run):
    """
    Save individual species metadata to avoid data loss if species fails mid-run.
    Each species gets its own metadata file.

    Args:
        assembly_id (str): Assembly ID
        list_metadata (dict): Metadata dictionary
        profile_data (dict): Profile configuration with Metadata_directory
        suffix_run (str): Suffix for the run
    """
    try:
        metadata_file = f"{profile_data['Metadata_directory']}metadata_{assembly_id}_run{suffix_run}.json"
        with open(metadata_file, 'w') as json_file:
            json.dump(list_metadata[assembly_id], json_file, indent=4)
    except Exception as e:
        log_warning(f"Could not save species metadata for {assembly_id}: {e}")

def run_species_workflows(assembly_id, gi, list_metadata, profile_data, workflow_data, is_resume=False):
    """
    Run VGP workflows in sequence for a species.
    Workflow 4 generates both hap1 and hap2, so workflows 8 and 9 run on both haplotypes in parallel.
    Workflow 9 waits for workflow 8 to complete before starting.

    Args:
        assembly_id (str): Assembly ID
        gi (GalaxyInstance): Galaxy instance object
        list_metadata (dict): Metadata dictionary
        profile_data (dict): Profile configuration
        workflow_data (dict): Workflow paths and info
        is_resume (bool): Whether this is a resume run (enables history invocation search)
    """
    command_lines={}
    galaxy_instance=profile_data['Galaxy_instance']
    galaxy_key=profile_data['Galaxy_key']
    species_name=list_metadata[assembly_id]['Name']
    history_name=list_metadata[assembly_id]['History_name']
    suffix_run=profile_data['Suffix']

    # Initialize invocation cache (built lazily when first needed to minimize API calls)
    history_invocation_cache = None

    # Try to get history_id (only during resume - for new runs it will be created by WF1)
    history_id = None
    if is_resume:
        history_id = get_or_find_history_id(gi, list_metadata, assembly_id, is_resume=is_resume)
        if history_id:
            print(f"Using Galaxy history: {history_id}")
        else:
            print(f"Note: No history found yet for {assembly_id}. Will retrieve from first invocation.")

    # Build command lines - use history_id if available (resume), otherwise use history_name
    command_lines = {}
    for key in workflow_data.keys():
        workflow_path=workflow_data[key]['Path']
        job_yaml=list_metadata[assembly_id]["job_files"][key]
        log_file=list_metadata[assembly_id]["planemo_logs"][key]
        res_file=list_metadata[assembly_id]["invocation_jsons"][key]

        # Use history_id if we have it (from resume), otherwise use history_name
        if history_id:
            command_lines[key]="planemo run "+workflow_path+" "+job_yaml+" --engine external_galaxy --galaxy_url "+galaxy_instance+" --simultaneous_uploads --check_uploads_ok --galaxy_user_key "+galaxy_key+" --history_id "+history_id+" --test_output_json "+res_file+" > "+log_file
        else:
            command_lines[key]="planemo run "+workflow_path+" "+job_yaml+" --engine external_galaxy --galaxy_url "+galaxy_instance+" --simultaneous_uploads --check_uploads_ok --galaxy_user_key "+galaxy_key+" --history_name "+history_name+" --test_output_json "+res_file+" > "+log_file

    # === WORKFLOW 1 ===
    invocation_wf1 = None

    # Try to get from JSON file
    if os.path.exists(list_metadata[assembly_id]["invocation_jsons"]["Workflow_1"]):
        wf1json = open(list_metadata[assembly_id]["invocation_jsons"]["Workflow_1"])
        reswf1 = json.load(wf1json)
        invocation_wf1 = reswf1["tests"][0]["data"]['invocation_details']['details']['invocation_id']
        list_metadata[assembly_id]["invocations"]["Workflow_1"] = invocation_wf1
        # Also extract history_id from JSON file to avoid unnecessary API calls
        if 'history_id' in reswf1["tests"][0]["data"]['invocation_details']:
            history_id = reswf1["tests"][0]["data"]['invocation_details']['history_id']
            list_metadata[assembly_id]['history_id'] = history_id
        print(f"Workflow 1 for {assembly_id} ({species_name}) result file found.\n")

    # Try to get from metadata
    elif list_metadata[assembly_id]["invocations"]["Workflow_1"] != 'NA':
        invocation_wf1 = list_metadata[assembly_id]["invocations"]["Workflow_1"]
        print(f"Workflow 1 for {assembly_id} ({species_name}) invocation found in metadata.\n")

    # If not found in metadata (batch update would have populated it if in history), launch the workflow
    if not invocation_wf1 or invocation_wf1 == 'NA':
        if is_resume:
            print(f"No previous run found for Workflow 1. Launching...")
        else:
            print(f"Launching Workflow 1 for {assembly_id}...")
        os.system(command_lines['Workflow_1'])
        print(f"Workflow 1 for {assembly_id} ({species_name}) has been launched.\n")

        # Wait for invocation JSON to be written (retry up to 30 seconds)
        wf1_json_path = list_metadata[assembly_id]["invocation_jsons"]["Workflow_1"]
        max_retries = 10
        retry_interval = 3  # seconds
        for attempt in range(max_retries):
            if os.path.exists(wf1_json_path):
                try:
                    with open(wf1_json_path) as wf1json:
                        reswf1 = json.load(wf1json)
                    invocation_wf1 = reswf1["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                    list_metadata[assembly_id]["invocations"]["Workflow_1"] = invocation_wf1
                    # Also extract history_id from JSON file to avoid unnecessary API calls
                    if 'history_id' in reswf1["tests"][0]["data"]['invocation_details']:
                        history_id = reswf1["tests"][0]["data"]['invocation_details']['history_id']
                        list_metadata[assembly_id]['history_id'] = history_id
                    print(f"Retrieved invocation ID for Workflow 1: {invocation_wf1}")
                    break
                except (json.JSONDecodeError, KeyError) as e:
                    if attempt < max_retries - 1:
                        print(f"Waiting for Workflow 1 invocation data to be written (attempt {attempt+1}/{max_retries})...")
                        time.sleep(retry_interval)
                    else:
                        print(f"Warning: Could not parse Workflow 1 JSON after {max_retries} attempts")
            else:
                if attempt < max_retries - 1:
                    print(f"Waiting for Workflow 1 invocation JSON file (attempt {attempt+1}/{max_retries})...")
                    time.sleep(retry_interval)

    # If we STILL don't have invocation (workflow just launched), can't proceed
    if not invocation_wf1 or invocation_wf1 == 'NA':
        print(f"Workflow 1 just launched for {assembly_id}, but invocation data is not yet available.")
        print(f"You can safely interrupt and resume later using the --resume flag.\n")
        return {assembly_id: list_metadata[assembly_id]}

    # Update history_id from invocation (most reliable method)
    new_history_id = get_or_find_history_id(gi, list_metadata, assembly_id, invocation_wf1, is_resume)

    # Update all command lines to use history_id if we just got it (wasn't available before)
    if new_history_id and not history_id:
        print(f"Updating commands to use history_id: {new_history_id}")
        history_id = new_history_id
        for key in workflow_data.keys():
            if key != "Workflow_1":  # WF1 already launched, don't update its command
                workflow_path = workflow_data[key]['Path']
                job_yaml = list_metadata[assembly_id]["job_files"][key]
                log_file = list_metadata[assembly_id]["planemo_logs"][key]
                res_file = list_metadata[assembly_id]["invocation_jsons"][key]
                command_lines[key] = "planemo run "+workflow_path+" "+job_yaml+" --engine external_galaxy --galaxy_url "+galaxy_instance+" --simultaneous_uploads --check_uploads_ok --galaxy_user_key "+galaxy_key+" --history_id "+history_id+" --test_output_json "+res_file+" > "+log_file
    elif new_history_id:
        history_id = new_history_id

    # Store dataset IDs for Workflow 1
    try:
        wf1_inv = gi.invocations.show_invocation(str(invocation_wf1))
        list_metadata[assembly_id]["dataset_ids"]["Workflow_1"] = get_datasets_ids(wf1_inv)
    except Exception as e:
        print(f"Warning: Could not retrieve dataset IDs for Workflow 1: {e}")

    # Save metadata after WF1 completes
    save_species_metadata(assembly_id, list_metadata, profile_data, suffix_run)

    # === WORKFLOW 4 ===
    # For --resume mode: Check if invocation is complete, poll if needed, then check outputs
    # For normal mode: Just proceed (trust planemo launched successfully)
    if is_resume:
        print(f"Checking Workflow 1 status for {assembly_id}...")
        # First, check if invocation is in a terminal state
        is_complete, state = check_invocation_complete(gi, invocation_wf1)
        print(f"Workflow 1 status: {state} (complete: {is_complete})")

        if not is_complete:
            # Invocation still running - poll until complete
            print(f"Workflow 1 for {assembly_id} is still running (state: {state})")
            poll_interval = profile_data.get('poll_interval_other', 60) * 60  # Convert minutes to seconds
            is_complete, state = poll_until_invocation_complete(gi, invocation_wf1, "Workflow 1", assembly_id, poll_interval=poll_interval)

            if not is_complete or state not in ['ok']:
                print(f"Workflow 1 for {assembly_id} did not complete successfully (state: {state})")
                if state in ['failed', 'cancelled', 'error']:
                    mark_invocation_as_failed(assembly_id, list_metadata, "Workflow_1", invocation_wf1, profile_data, suffix_run)
                return {assembly_id: list_metadata[assembly_id]}

        # Now check if required outputs exist
        required_wf1_outputs = ['Collection of Pacbio Data', 'Merged Meryl Database', 'GenomeScope summary', 'GenomeScope Model Parameters']
        outputs_ready, missing_outputs = check_required_outputs_exist(gi, invocation_wf1, required_wf1_outputs)

        if not outputs_ready:
            print(f"Workflow 1 for {assembly_id}: Required outputs for WF4 not yet ready. Missing: {', '.join(missing_outputs)}")
            print(f"Resume again later when Workflow 1 has progressed further.")
            return {assembly_id: list_metadata[assembly_id]}
        else:
            print(f"Required outputs from Workflow 1 are ready for {assembly_id}. Proceeding with Workflow 4.\n")
    else:
        # Normal mode: WF1 ready (either found or just launched), proceed to WF4
        print(f"Workflow 1 ready for {assembly_id}. Proceeding with Workflow 4.\n")

    # Try to get Workflow 4 invocation
    invocation_wf4 = None

    # Try to get from JSON file
    if os.path.exists(list_metadata[assembly_id]["invocation_jsons"]["Workflow_4"]):
        wf4json = open(list_metadata[assembly_id]["invocation_jsons"]["Workflow_4"])
        reswf4 = json.load(wf4json)
        invocation_wf4 = reswf4["tests"][0]["data"]['invocation_details']['details']['invocation_id']
        list_metadata[assembly_id]["invocations"]["Workflow_4"] = invocation_wf4
        print(f"Workflow 4 for {assembly_id} result file found.\n")

    # Try to get from metadata
    elif list_metadata[assembly_id]["invocations"]["Workflow_4"] != 'NA':
        invocation_wf4 = list_metadata[assembly_id]["invocations"]["Workflow_4"]
        print(f"Workflow 4 for {assembly_id} invocation found in metadata.\n")

    # Try to fetch from history (only during resume, prerequisite WF1 exists)
    else:
        if is_resume and history_id:
            # Build invocation cache if not already built (first time we need it)
            if history_invocation_cache is None:
                print(f"Building invocation cache for {assembly_id}...")
                history_invocation_cache = build_invocation_cache(gi, history_id)

            print(f"Searching history for Workflow 4 invocation for {assembly_id}...")
            invocation_wf4 = fetch_invocation_from_history(gi, history_id, "VGP4", cache=history_invocation_cache)
            if invocation_wf4:
                print(f"Found invocation {invocation_wf4} in history")
                list_metadata[assembly_id]["invocations"]["Workflow_4"] = invocation_wf4
                # Store dataset IDs for this invocation
                try:
                    wf4_inv = gi.invocations.show_invocation(str(invocation_wf4))
                    list_metadata[assembly_id]["dataset_ids"]["Workflow_4"] = get_datasets_ids(wf4_inv)
                    print(f"Retrieved dataset IDs for Workflow 4")
                except Exception as e:
                    print(f"Warning: Could not retrieve dataset IDs for Workflow 4: {e}")
                # Save metadata after finding invocation
                save_species_metadata(assembly_id, list_metadata, profile_data, suffix_run)

    # If not found, prepare and launch
    if not invocation_wf4 or invocation_wf4 == 'NA':
        if is_resume:
            print(f"No previous run found for Workflow 4. Preparing and launching...")
        else:
            print(f"Preparing and launching Workflow 4 for {assembly_id}...")
        prepare_yaml_wf4(assembly_id, list_metadata, profile_data)
        os.system(command_lines["Workflow_4"])
        print(f"Workflow 4 for {assembly_id} ({species_name}) has been launched.\n")

        # Wait for invocation JSON to be written (retry up to 30 seconds)
        wf4_json_path = list_metadata[assembly_id]["invocation_jsons"]["Workflow_4"]
        max_retries = 10
        retry_interval = 3  # seconds
        for attempt in range(max_retries):
            if os.path.exists(wf4_json_path):
                try:
                    with open(wf4_json_path) as wf4json:
                        reswf4 = json.load(wf4json)
                    invocation_wf4 = reswf4["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                    list_metadata[assembly_id]["invocations"]["Workflow_4"] = invocation_wf4
                    print(f"Retrieved invocation ID for Workflow 4: {invocation_wf4}")
                    break
                except (json.JSONDecodeError, KeyError) as e:
                    if attempt < max_retries - 1:
                        print(f"Waiting for Workflow 4 invocation data to be written (attempt {attempt+1}/{max_retries})...")
                        time.sleep(retry_interval)
                    else:
                        print(f"Warning: Could not parse Workflow 4 JSON after {max_retries} attempts")
            else:
                if attempt < max_retries - 1:
                    print(f"Waiting for Workflow 4 invocation JSON file (attempt {attempt+1}/{max_retries})...")
                    time.sleep(retry_interval)

    # If we STILL don't have invocation, can't proceed
    if not invocation_wf4 or invocation_wf4 == 'NA':
        print(f"Workflow 4 just launched for {assembly_id}, but invocation data is not yet available.")
        print(f"You can safely interrupt and resume later using the --resume flag.\n")
        return {assembly_id: list_metadata[assembly_id]}

    # Get workflow 4 invocation object for preparing downstream workflows
    try:
        wf4_inv=gi.invocations.show_invocation(str(invocation_wf4))
    except Exception as e:
        print(f"Warning: Could not get Workflow 4 invocation details: {e}")
        print(f"Workflow 4 may still be initializing. Waiting for Galaxy to register the invocation.")
        print(f"You can safely interrupt and resume later using the --resume flag.\n")
        return {assembly_id: list_metadata[assembly_id]}

    # Store dataset IDs for Workflow 4
    try:
        list_metadata[assembly_id]["dataset_ids"]["Workflow_4"] = get_datasets_ids(wf4_inv)
    except Exception as e:
        print(f"Warning: Could not retrieve dataset IDs for Workflow 4: {e}")

    # Save metadata after WF4 completes
    save_species_metadata(assembly_id, list_metadata, profile_data, suffix_run)

    # === WORKFLOW 0 (Mitochondrial) - runs right after workflow 4 is launched (doesn't wait for completion) ===
    print(f"Workflow 4 ready for {assembly_id}. Proceeding with Workflow 0.\n")

    # Try to get Workflow 0 invocation
    invocation_wf0 = None

    # Try to get from JSON file
    if os.path.exists(list_metadata[assembly_id]["invocation_jsons"]["Workflow_0"]):
        wf0json = open(list_metadata[assembly_id]["invocation_jsons"]["Workflow_0"])
        reswf0 = json.load(wf0json)
        invocation_wf0 = reswf0["tests"][0]["data"]['invocation_details']['details']['invocation_id']
        list_metadata[assembly_id]["invocations"]["Workflow_0"] = invocation_wf0
        print(f"Workflow 0 for {assembly_id} result file found.\n")

    # Try to get from metadata
    elif list_metadata[assembly_id]["invocations"]["Workflow_0"] != 'NA':
        invocation_wf0 = list_metadata[assembly_id]["invocations"]["Workflow_0"]
        print(f"Workflow 0 for {assembly_id} invocation found in metadata.\n")

    # Try to fetch from history (only during resume, prerequisite WF4 exists)
    else:
        if is_resume and history_id:
            # Build invocation cache if not already built
            if history_invocation_cache is None:
                print(f"Building invocation cache for {assembly_id}...")
                history_invocation_cache = build_invocation_cache(gi, history_id)

            print(f"Searching history for Workflow 0 invocation for {assembly_id}...")
            invocation_wf0 = fetch_invocation_from_history(gi, history_id, "VGP0", cache=history_invocation_cache)
            if invocation_wf0:
                print(f"Found invocation {invocation_wf0} in history")
                list_metadata[assembly_id]["invocations"]["Workflow_0"] = invocation_wf0
                # Store dataset IDs for this invocation
                try:
                    wf0_inv = gi.invocations.show_invocation(str(invocation_wf0))
                    list_metadata[assembly_id]["dataset_ids"]["Workflow_0"] = get_datasets_ids(wf0_inv)
                    print(f"Retrieved dataset IDs for Workflow 0")
                except Exception as e:
                    print(f"Warning: Could not retrieve dataset IDs for Workflow 0: {e}")
                # Save metadata after finding invocation
                save_species_metadata(assembly_id, list_metadata, profile_data, suffix_run)

    # If not found, prepare and launch (non-blocking for other workflows)
    if not invocation_wf0 or invocation_wf0 == 'NA':
        if is_resume:
            print(f"No previous run found for Workflow 0. Preparing and launching...")
        else:
            print(f"Preparing and launching Workflow 0 for {assembly_id}...")
        prepare_yaml_wf0(assembly_id, list_metadata, wf4_inv, profile_data)
        os.system(command_lines["Workflow_0"])
        print(f"Workflow 0 for {assembly_id} ({species_name}) has been launched.\n")

        # Try to get from newly created JSON
        if os.path.exists(list_metadata[assembly_id]["invocation_jsons"]["Workflow_0"]):
            wf0json = open(list_metadata[assembly_id]["invocation_jsons"]["Workflow_0"])
            reswf0 = json.load(wf0json)
            invocation_wf0 = reswf0["tests"][0]["data"]['invocation_details']['details']['invocation_id']
            list_metadata[assembly_id]["invocations"]["Workflow_0"] = invocation_wf0

    # Store dataset IDs for Workflow 0 if invocation exists
    if invocation_wf0 and invocation_wf0 != 'NA':
        try:
            wf0_inv = gi.invocations.show_invocation(str(invocation_wf0))
            list_metadata[assembly_id]["dataset_ids"]["Workflow_0"] = get_datasets_ids(wf0_inv)
        except Exception as e:
            print(f"Warning: Could not retrieve dataset IDs for Workflow 0: {e}")

    # Save metadata after WF0 is launched/found
    save_species_metadata(assembly_id, list_metadata, profile_data, suffix_run)

    # Haplotype mapping - workflow 4 generates hap1 and hap2
    hap_mapping = {
        'hap1': 'Haplotype 1',
        'hap2': 'Haplotype 2'
    }

    # === WORKFLOW 8 (Both Haplotypes in Parallel) ===
    # For --resume mode: Check if invocation is complete, poll if needed, then check outputs
    # For normal mode: Just proceed (trust planemo launched successfully)
    if is_resume:
        # First, check if invocation is in a terminal state
        is_complete, state = check_invocation_complete(gi, invocation_wf4)

        if not is_complete:
            # Invocation still running - poll until complete
            print(f"Workflow 4 for {assembly_id} is still running (state: {state})")
            poll_interval = profile_data.get('poll_interval_other', 60) * 60  # Convert minutes to seconds
            is_complete, state = poll_until_invocation_complete(gi, invocation_wf4, "Workflow 4", assembly_id, poll_interval=poll_interval)

            if not is_complete or state not in ['ok']:
                print(f"Workflow 4 for {assembly_id} did not complete successfully (state: {state})")
                if state in ['failed', 'cancelled', 'error']:
                    mark_invocation_as_failed(assembly_id, list_metadata, "Workflow_4", invocation_wf4, profile_data, suffix_run)
                return {assembly_id: list_metadata[assembly_id]}

        # Now check if required outputs exist (needed for WF8)
        required_wf4_outputs = ['usable hap1 gfa', 'usable hap2 gfa', 'Estimated Genome size', 'Trimmed Hi-C reads']
        outputs_ready, missing_outputs = check_required_outputs_exist(gi, invocation_wf4, required_wf4_outputs)

        if not outputs_ready:
            print(f"Workflow 4 for {assembly_id}: Required outputs for WF8 not yet ready. Missing: {', '.join(missing_outputs)}")
            # Poll until outputs are ready
            poll_interval = profile_data.get('poll_interval_outputs', 60) * 60  # Convert minutes to seconds (default: 1 hour)
            outputs_ready, missing_outputs = poll_until_outputs_ready(
                gi, invocation_wf4, required_wf4_outputs,
                "Workflow 4", assembly_id, poll_interval=poll_interval
            )

            if not outputs_ready:
                print(f"Workflow 4 for {assembly_id}: Required outputs not ready after polling.")
                print(f"Resume again later when Workflow 4 has progressed further.")
                return {assembly_id: list_metadata[assembly_id]}

        print(f"\n=== Required outputs from Workflow 4 are ready for {assembly_id}. Preparing Workflow 8 for both haplotypes ===\n")
    else:
        # Normal mode: WF4 ready (either found or just launched), proceed to WF8
        print(f"\n=== Workflow 4 ready for {assembly_id}. Preparing Workflow 8 for both haplotypes ===\n")

    # Try to get invocations for both haplotypes
    wf8_invocations = {}
    wf8_to_launch = []  # Track which ones need to be launched

    for hap_code in ['hap1', 'hap2']:
        haplotype_name = hap_mapping[hap_code]
        wf8_key = f"Workflow_8_{hap_code}"
        invocation_wf8 = None

        # Try to get from JSON file
        wf8_json_path = list_metadata[assembly_id]["invocation_jsons"].get(wf8_key)
        if wf8_json_path and os.path.exists(wf8_json_path):
            wf8json = open(wf8_json_path)
            reswf8 = json.load(wf8json)
            invocation_wf8 = reswf8["tests"][0]["data"]['invocation_details']['details']['invocation_id']
            list_metadata[assembly_id]["invocations"][wf8_key] = invocation_wf8
            print(f"Workflow 8 ({haplotype_name}) for {assembly_id} result file found.\n")

        # Try to get from metadata
        elif list_metadata[assembly_id]["invocations"].get(wf8_key, 'NA') != 'NA':
            invocation_wf8 = list_metadata[assembly_id]["invocations"][wf8_key]
            print(f"Workflow 8 ({haplotype_name}) for {assembly_id} invocation found in metadata.\n")

        # Try to fetch from history (only during resume, prerequisite WF4 exists)
        else:
            if is_resume and history_id:
                # Build invocation cache if not already built
                if history_invocation_cache is None:
                    print(f"Building invocation cache for {assembly_id}...")
                    history_invocation_cache = build_invocation_cache(gi, history_id)

                print(f"Searching history for Workflow 8 ({haplotype_name}) invocation for {assembly_id}...")
                invocation_wf8 = fetch_invocation_from_history(gi, history_id, "VGP8", haplotype=haplotype_name, cache=history_invocation_cache)
                if invocation_wf8:
                    print(f"Found invocation {invocation_wf8} in history")
                    list_metadata[assembly_id]["invocations"][wf8_key] = invocation_wf8
                    # Store dataset IDs for this invocation
                    try:
                        wf8_inv = gi.invocations.show_invocation(str(invocation_wf8))
                        list_metadata[assembly_id]["dataset_ids"][wf8_key] = get_datasets_ids(wf8_inv)
                        print(f"Retrieved dataset IDs for Workflow 8 ({haplotype_name})")
                    except Exception as e:
                        print(f"Warning: Could not retrieve dataset IDs for Workflow 8 ({haplotype_name}): {e}")
                    # Save metadata after finding invocation
                    save_species_metadata(assembly_id, list_metadata, profile_data, suffix_run)

        # Mark for launch if not found
        if not invocation_wf8 or invocation_wf8 == 'NA':
            wf8_to_launch.append(hap_code)
        else:
            wf8_invocations[hap_code] = invocation_wf8

    # Prepare YAML for workflows that need to be launched
    if wf8_to_launch:
        for hap_code in wf8_to_launch:
            prepare_yaml_wf8(assembly_id, list_metadata, wf4_inv, profile_data, hap_code)

    # Launch workflows
    for hap_code in wf8_to_launch:
        haplotype_name = hap_mapping[hap_code]
        wf8_key = f"Workflow_8_{hap_code}"

        if is_resume:
            print(f"No previous run found for Workflow 8 ({haplotype_name}). Launching...")
        else:
            print(f"Launching Workflow 8 ({haplotype_name}) for {assembly_id}...")
        if wf8_key in command_lines:
            os.system(command_lines[wf8_key])
            print(f"Workflow 8 ({haplotype_name}) for {assembly_id} ({species_name}) has been launched.\n")

            # Wait for invocation JSON to be written (retry up to 30 seconds)
            wf8_json_path = list_metadata[assembly_id]["invocation_jsons"].get(wf8_key)
            if wf8_json_path:
                max_retries = 10
                retry_interval = 3  # seconds
                invocation_wf8 = None
                for attempt in range(max_retries):
                    if os.path.exists(wf8_json_path):
                        try:
                            with open(wf8_json_path) as wf8json:
                                reswf8 = json.load(wf8json)
                            invocation_wf8 = reswf8["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                            list_metadata[assembly_id]["invocations"][wf8_key] = invocation_wf8
                            wf8_invocations[hap_code] = invocation_wf8
                            print(f"Retrieved invocation ID for Workflow 8 ({haplotype_name}): {invocation_wf8}")
                            break
                        except (json.JSONDecodeError, KeyError) as e:
                            if attempt < max_retries - 1:
                                print(f"Waiting for Workflow 8 ({haplotype_name}) invocation data (attempt {attempt+1}/{max_retries})...")
                                time.sleep(retry_interval)
                            else:
                                print(f"Warning: Could not parse Workflow 8 ({haplotype_name}) JSON after {max_retries} attempts")
                    else:
                        if attempt < max_retries - 1:
                            print(f"Waiting for Workflow 8 ({haplotype_name}) JSON file (attempt {attempt+1}/{max_retries})...")
                            time.sleep(retry_interval)

    # Store dataset IDs for Workflow 8 (both haplotypes)
    for hap_code, inv_id in wf8_invocations.items():
        if inv_id:
            wf8_key = f"Workflow_8_{hap_code}"
            try:
                wf8_inv = gi.invocations.show_invocation(str(inv_id))
                list_metadata[assembly_id]["dataset_ids"][wf8_key] = get_datasets_ids(wf8_inv)
            except Exception as e:
                print(f"Warning: Could not retrieve dataset IDs for {wf8_key}: {e}")

    # Save metadata after WF8 is launched/found for both haplotypes
    save_species_metadata(assembly_id, list_metadata, profile_data, suffix_run)

    # === WORKFLOW 9 (Both Haplotypes in Parallel - after workflow 8 completes) ===
    # Check if both Workflow 8 invocations exist
    if not wf8_invocations:
        print(f"No Workflow 8 invocations found for {assembly_id}. Waiting for Workflow 8 to be launched.")
        print(f"Invocation data has been saved. You can safely interrupt and resume later using the --resume flag.\n")
        return {assembly_id: list_metadata[assembly_id]}

    # For --resume mode: Check if invocations are complete, poll if needed, then check outputs
    # For normal mode: Just proceed (trust planemo launched WF8 successfully)
    if is_resume:
        # First, check if all WF8 invocations are in terminal states
        all_complete = True
        poll_needed = {}

        for hap_code, inv_id in wf8_invocations.items():
            is_complete, state = check_invocation_complete(gi, inv_id)
            if not is_complete:
                all_complete = False
                poll_needed[hap_code] = (inv_id, state)

        # Poll any incomplete invocations
        if not all_complete:
            print(f"Some Workflow 8 invocations for {assembly_id} are still running:")
            for hap_code, (inv_id, state) in poll_needed.items():
                print(f"  {hap_mapping[hap_code]}: state = {state}")

            poll_interval = profile_data.get('poll_interval_other', 60) * 60  # Convert minutes to seconds

            # Poll each incomplete invocation
            for hap_code, (inv_id, state) in poll_needed.items():
                print(f"\nPolling Workflow 8 ({hap_mapping[hap_code]})...")
                is_complete, final_state = poll_until_invocation_complete(
                    gi, inv_id, f"Workflow 8 ({hap_mapping[hap_code]})", assembly_id, poll_interval=poll_interval
                )

                if not is_complete or final_state not in ['ok']:
                    print(f"Workflow 8 ({hap_mapping[hap_code]}) for {assembly_id} did not complete successfully (state: {final_state})")
                    if final_state in ['failed', 'cancelled', 'error']:
                        wf8_key = f"Workflow_8_{hap_code}"
                        mark_invocation_as_failed(assembly_id, list_metadata, wf8_key, inv_id, profile_data, suffix_run)
                    return {assembly_id: list_metadata[assembly_id]}

        # Now check if required outputs exist from all haplotypes
        required_wf8_outputs = ['Reconciliated Scaffolds: fasta']
        all_outputs_ready = True
        wf8_outputs_status = {}

        for hap_code, inv_id in wf8_invocations.items():
            outputs_ready, missing_outputs = check_required_outputs_exist(gi, inv_id, required_wf8_outputs)
            wf8_outputs_status[hap_code] = (outputs_ready, missing_outputs)
            if not outputs_ready:
                all_outputs_ready = False
                print(f"Required outputs for WF9 not yet ready from Workflow 8 ({hap_mapping[hap_code]}) for {assembly_id}. Missing: {', '.join(missing_outputs)}")

        if not all_outputs_ready:
            print(f"Workflow 8 for {assembly_id}: Required outputs for WF9 not yet ready.")
            for hap_code, (outputs_ready, missing_outputs) in wf8_outputs_status.items():
                if not outputs_ready:
                    print(f"  Haplotype {hap_mapping[hap_code]}: Missing: {', '.join(missing_outputs)}")
            print(f"Resume again later when Workflow 8 has progressed further.")
            return {assembly_id: list_metadata[assembly_id]}
        else:
            print(f"\n=== Required outputs from all Workflow 8 invocations are ready for {assembly_id}. Preparing Workflow 9 for both haplotypes ===\n")
    else:
        # Normal mode: WF8 ready (either found or just launched), proceed to WF9
        print(f"Workflow 8 ready for {assembly_id}. Proceeding with Workflow 9.\n")

    # Get configuration for workflow 9
    path_script = profile_data.get('path_script', os.path.dirname(__file__))
    wf9_version = profile_data.get('wf9_version', 'fcs')  # 'fcs' or 'legacy'
    if wf9_version == 'fcs':
        template_file = path_script + "/templates/wf9_run_sample_fcs.yaml"
    else:
        template_file = path_script + "/templates/wf9_run_sample_legacy.yaml"

    # Try to get invocations for both haplotypes
    wf9_invocations = {}
    wf9_to_launch = []  # Track which ones need to be launched

    for hap_code in ['hap1', 'hap2']:
        haplotype_name = hap_mapping[hap_code]
        wf9_key = f"Workflow_9_{hap_code}"
        invocation_wf9 = None

        # Try to get from JSON file
        wf9_json_path = list_metadata[assembly_id]["invocation_jsons"].get(wf9_key)
        if wf9_json_path and os.path.exists(wf9_json_path):
            wf9json = open(wf9_json_path)
            reswf9 = json.load(wf9json)
            invocation_wf9 = reswf9["tests"][0]["data"]['invocation_details']['details']['invocation_id']
            list_metadata[assembly_id]["invocations"][wf9_key] = invocation_wf9
            print(f"Workflow 9 ({haplotype_name}) for {assembly_id} result file found.\n")

        # Try to get from metadata
        elif list_metadata[assembly_id]["invocations"].get(wf9_key, 'NA') != 'NA':
            invocation_wf9 = list_metadata[assembly_id]["invocations"][wf9_key]
            print(f"Workflow 9 ({haplotype_name}) for {assembly_id} invocation found in metadata.\n")

        # Try to fetch from history (only during resume, prerequisite WF8 for this haplotype exists)
        else:
            if is_resume and history_id and hap_code in wf8_invocations:
                # Build invocation cache if not already built
                if history_invocation_cache is None:
                    print(f"Building invocation cache for {assembly_id}...")
                    history_invocation_cache = build_invocation_cache(gi, history_id)

                print(f"Searching history for Workflow 9 ({haplotype_name}) invocation for {assembly_id}...")
                invocation_wf9 = fetch_invocation_from_history(gi, history_id, "VGP9", haplotype=haplotype_name, cache=history_invocation_cache)
                if invocation_wf9:
                    print(f"Found invocation {invocation_wf9} in history")
                    list_metadata[assembly_id]["invocations"][wf9_key] = invocation_wf9
                    # Store dataset IDs for this invocation
                    try:
                        wf9_inv = gi.invocations.show_invocation(str(invocation_wf9))
                        list_metadata[assembly_id]["dataset_ids"][wf9_key] = get_datasets_ids(wf9_inv)
                        print(f"Retrieved dataset IDs for Workflow 9 ({haplotype_name})")
                    except Exception as e:
                        print(f"Warning: Could not retrieve dataset IDs for Workflow 9 ({haplotype_name}): {e}")
                    # Save metadata after finding invocation
                    save_species_metadata(assembly_id, list_metadata, profile_data, suffix_run)

        # Mark for launch if not found
        if not invocation_wf9 or invocation_wf9 == 'NA':
            wf9_to_launch.append(hap_code)
        else:
            wf9_invocations[hap_code] = invocation_wf9

    # Query taxon ID from NCBI dataset once per species if using FCS version
    taxon_id = None
    if wf9_version == 'fcs' and wf9_to_launch:
        # Check if taxon_id already exists in metadata (from previous run)
        if 'taxon_id' in list_metadata[assembly_id] and list_metadata[assembly_id]['taxon_id'] != 'NA':
            taxon_id = list_metadata[assembly_id]['taxon_id']
            print(f"Using cached taxon ID {taxon_id} for {species_name}")
        else:
            # Query NCBI dataset
            species_name_for_ncbi = species_name.replace("_", " ")
            try:
                datasets_command = ['datasets', 'summary', 'taxonomy', 'taxon', species_name_for_ncbi, '--as-json-lines']
                data_type = subprocess.run(datasets_command, capture_output=True, text=True, check=True)
                taxon_data = json.loads(data_type.stdout)
                taxon_id = str(taxon_data['taxonomy']['tax_id'])
                taxon_name = taxon_data['taxonomy']['current_scientific_name']['name']
                print(f"Retrieved taxon ID {taxon_id} for {species_name} ({taxon_name})")

                # Store in metadata for future runs
                list_metadata[assembly_id]['taxon_id'] = taxon_id
            except Exception as e:
                print(f"Error querying NCBI dataset for {species_name}: {e}")
                print(f"Please check you have the latest version of the NCBI datasets tool installed.")
                print(f"https://www.ncbi.nlm.nih.gov/datasets/docs/v2/command-line-tools/download-and-install/")
                raise SystemExit(f"Failed to get taxon ID for {species_name}")

    # Prepare YAML files for workflows that need to be launched
    for hap_code in wf9_to_launch:
        if hap_code not in wf8_invocations:
            print(f"Warning: No invocation found for Workflow 8 ({hap_mapping[hap_code]}) - skipping Workflow 9")
            continue

        haplotype_name = hap_mapping[hap_code]
        wf8_inv = gi.invocations.show_invocation(str(wf8_invocations[hap_code]))
        wf9_yaml = list_metadata[assembly_id]["job_files"].get(f"Workflow_9_{hap_code}")

        if wf9_yaml:
            prepare_yaml_wf9(
                assembly_id=assembly_id,
                species_name=species_name,
                invocation_wf8=wf8_inv,
                haplotype=haplotype_name,
                output_file=wf9_yaml,
                template_file=template_file,
                taxon_ID=taxon_id
            )

    # Launch workflows
    for hap_code in wf9_to_launch:
        haplotype_name = hap_mapping[hap_code]
        wf9_key = f"Workflow_9_{hap_code}"

        if is_resume:
            print(f"No previous run found for Workflow 9 ({haplotype_name}). Launching...")
        else:
            print(f"Launching Workflow 9 ({haplotype_name}) for {assembly_id}...")
        if wf9_key in command_lines:
            os.system(command_lines[wf9_key])
            print(f"Workflow 9 ({haplotype_name}) for {assembly_id} ({species_name}) has been launched.\n")

            # Wait for invocation JSON to be written (retry up to 30 seconds)
            wf9_json_path = list_metadata[assembly_id]["invocation_jsons"].get(wf9_key)
            if wf9_json_path:
                max_retries = 10
                retry_interval = 3  # seconds
                invocation_wf9 = None
                for attempt in range(max_retries):
                    if os.path.exists(wf9_json_path):
                        try:
                            with open(wf9_json_path) as wf9json:
                                reswf9 = json.load(wf9json)
                            invocation_wf9 = reswf9["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                            list_metadata[assembly_id]["invocations"][wf9_key] = invocation_wf9
                            wf9_invocations[hap_code] = invocation_wf9
                            print(f"Retrieved invocation ID for Workflow 9 ({haplotype_name}): {invocation_wf9}")
                            break
                        except (json.JSONDecodeError, KeyError) as e:
                            if attempt < max_retries - 1:
                                print(f"Waiting for Workflow 9 ({haplotype_name}) invocation data (attempt {attempt+1}/{max_retries})...")
                                time.sleep(retry_interval)
                            else:
                                print(f"Warning: Could not parse Workflow 9 ({haplotype_name}) JSON after {max_retries} attempts")
                    else:
                        if attempt < max_retries - 1:
                            print(f"Waiting for Workflow 9 ({haplotype_name}) JSON file (attempt {attempt+1}/{max_retries})...")
                            time.sleep(retry_interval)

    # Store dataset IDs for Workflow 9 (both haplotypes)
    for hap_code, inv_id in wf9_invocations.items():
        if inv_id:
            wf9_key = f"Workflow_9_{hap_code}"
            try:
                wf9_inv = gi.invocations.show_invocation(str(inv_id))
                list_metadata[assembly_id]["dataset_ids"][wf9_key] = get_datasets_ids(wf9_inv)
            except Exception as e:
                print(f"Warning: Could not retrieve dataset IDs for {wf9_key}: {e}")

    # === PRE-CURATION WORKFLOW (OPTIONAL) ===
    invocation_precuration = None
    precuration_to_launch = False

    # Check if PreCuration workflow is configured
    if "Workflow_PreCuration" in workflow_data:
        print(f"\n--- Pre-Curation workflow ---")

        # Check if all required workflows completed successfully
        required_workflows = ["Workflow_4", "Workflow_9_hap1", "Workflow_9_hap2"]
        all_required_complete = all(
            key in list_metadata[assembly_id]["invocations"] and
            list_metadata[assembly_id]["invocations"][key] not in ['NA', None]
            for key in required_workflows
        )

        if not all_required_complete:
            print(f"Skipping pre-curation for {assembly_id}: required workflows (WF4, WF9 hap1/hap2) not completed")
        else:
            # Try to get from JSON file
            if os.path.exists(list_metadata[assembly_id]["invocation_jsons"]["Workflow_PreCuration"]):
                precuration_json = open(list_metadata[assembly_id]["invocation_jsons"]["Workflow_PreCuration"])
                res_precuration = json.load(precuration_json)
                invocation_precuration = res_precuration["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                list_metadata[assembly_id]["invocations"]["Workflow_PreCuration"] = invocation_precuration
                print(f"Pre-curation workflow for {assembly_id} result file found.\n")

            # Otherwise check if invocation exists but result file missing
            elif "Workflow_PreCuration" in list_metadata[assembly_id]["invocations"] and \
                 list_metadata[assembly_id]["invocations"]["Workflow_PreCuration"] not in ['NA', None]:
                invocation_precuration = list_metadata[assembly_id]["invocations"]["Workflow_PreCuration"]
                print(f"Pre-curation workflow for {assembly_id} invocation found: {invocation_precuration}.\n")

            # Otherwise need to launch
            else:
                precuration_to_launch = True

            # Launch pre-curation workflow if needed
            if precuration_to_launch:
                print(f"Launching pre-curation workflow for {assembly_id}...")

                # Get invocations for WF4 and WF9 haplotypes
                inv_wf4 = gi.invocations.show_invocation(str(list_metadata[assembly_id]["invocations"]["Workflow_4"]))
                inv_wf9_hap1 = gi.invocations.show_invocation(str(list_metadata[assembly_id]["invocations"]["Workflow_9_hap1"]))
                inv_wf9_hap2 = gi.invocations.show_invocation(str(list_metadata[assembly_id]["invocations"]["Workflow_9_hap2"]))

                # Get template file
                path_script = os.path.dirname(os.path.abspath(__file__))
                template_file = f"{path_script}/templates/precuration_run.sample.yaml"

                # Prepare YAML file
                job_file = list_metadata[assembly_id]["job_files"]["Workflow_PreCuration"]
                prepare_yaml_precuration(
                    assembly_id,
                    inv_wf4,
                    inv_wf9_hap1,
                    inv_wf9_hap2,
                    job_file,
                    template_file
                )

                # Launch workflow (planemo will block until complete since --no-wait was removed)
                os.system(command_lines["Workflow_PreCuration"])

                # Get invocation ID from completed JSON
                precuration_json = open(list_metadata[assembly_id]["invocation_jsons"]["Workflow_PreCuration"])
                res_precuration = json.load(precuration_json)
                invocation_precuration = res_precuration["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                list_metadata[assembly_id]["invocations"]["Workflow_PreCuration"] = invocation_precuration
                print(f"Pre-curation workflow launched: {invocation_precuration}\n")

            # Store dataset IDs for PreCuration
            if invocation_precuration:
                try:
                    precuration_inv = gi.invocations.show_invocation(str(invocation_precuration))
                    list_metadata[assembly_id]["dataset_ids"]["Workflow_PreCuration"] = get_datasets_ids(precuration_inv)
                except Exception as e:
                    print(f"Warning: Could not retrieve dataset IDs for PreCuration: {e}")

    # Save final metadata after all workflows complete
    save_species_metadata(assembly_id, list_metadata, profile_data, suffix_run)

    return {assembly_id: list_metadata[assembly_id]}

def process_species_wrapper(species_id, list_metadata, profile_data, dico_workflows, results_lock, results_status, is_resume=False):
    """
    Wrapper function to process a species and handle errors.
    Thread-safe function for parallel processing.
    Creates its own Galaxy instance to avoid pickling issues.

    Args:
        species_id (str): Assembly ID
        list_metadata (dict): Metadata dictionary (shared across threads)
        profile_data (dict): Profile configuration (must contain 'Galaxy_instance' and 'Galaxy_key')
        dico_workflows (dict): Workflow paths and info
        results_lock (threading.Lock): Lock for thread-safe updates
        results_status (dict): Results status dictionary (shared across threads)
        is_resume (bool): Whether this is a resume run (enables history invocation search)

    Returns:
        tuple: (species_id, status, error_message)
    """
    try:
        print(f"\n{'='*60}")
        print(f"Thread started for species: {species_id}")
        print(f"{'='*60}\n")

        # Create Galaxy instance for this thread/process
        gi = GalaxyInstance(profile_data['Galaxy_instance'], profile_data['Galaxy_key'])

        result = run_species_workflows(species_id, gi, list_metadata, profile_data, dico_workflows, is_resume)

        # Thread-safe update of shared metadata
        with results_lock:
            if result and species_id in result:
                list_metadata[species_id] = result[species_id]
                results_status[species_id] = "completed"
                print(f"\n✓ Successfully completed: {species_id}\n")

                # Save updated global metadata file
                try:
                    suffix_run = profile_data.get('Suffix', '')
                    metadata_file = f"{profile_data['Metadata_directory']}metadata_run{suffix_run}.json"
                    with open(metadata_file, 'w') as json_file:
                        json.dump(list_metadata, json_file, indent=4)
                    print(f"Updated global metadata file after {species_id} completion")

                    # Delete per-species file after successful merge
                    species_metadata_file = f"{profile_data['Metadata_directory']}metadata_{species_id}_run{suffix_run}.json"
                    if os.path.exists(species_metadata_file):
                        os.remove(species_metadata_file)
                        print(f"Removed per-species metadata file for {species_id}")
                except Exception as e:
                    print(f"Warning: Could not update global metadata or remove per-species file for {species_id}: {e}")
            else:
                results_status[species_id] = "completed (no result data)"
                print(f"\n⚠ Completed with warnings: {species_id}\n")

        return (species_id, "success", None)

    except Exception as e:
        error_msg = f"Error: {str(e)}"
        print(f"\n✗ Failed processing {species_id}: {error_msg}\n")
        with results_lock:
            results_status[species_id] = f"error: {str(e)}"
        return (species_id, "error", error_msg)

def prepare_yaml_wf4(assembly_id, list_metadata, profile_data):
    str_hic=""
    hic_f=list_metadata[assembly_id]['HiC_forward_reads']
    hic_r=list_metadata[assembly_id]['HiC_reverse_reads']
    gi=GalaxyInstance(profile_data['Galaxy_instance'], profile_data['Galaxy_key'])
    wf1_inv=gi.invocations.show_invocation(str(list_metadata[assembly_id]["invocations"]["Workflow_1"]))
    dic_data_ids=get_datasets_ids(wf1_inv)
    species_name=list_metadata[assembly_id]['Name']
    species_id=assembly_id
    hic_type=list_metadata[assembly_id]['HiC_Type']
    path_script=profile_data['path_script']
    for i in range(0,len(list_metadata[assembly_id]['HiC_forward_reads'])):
        namef=re.sub(r"\.f(ast)?q(sanger)?\.gz","",hic_f[i])
        namer=re.sub(r"\.f(ast)?q(sanger)?\.gz","",hic_r[i])
        str_hic=str_hic+"\n  - class: Collection\n    type: paired\n    identifier: "+namef+"\n    elements:\n    - identifier: forward\n      class: File\n      path: gxfiles://genomeark/species/"+species_name+"/"+species_id+"/genomic_data/"+hic_type+"/"+hic_f[i]+"\n      filetype: fastqsanger.gz\n    - identifier: reverse\n      class: File\n      path: gxfiles://genomeark/species/"+species_name+"/"+species_id+"/genomic_data/"+hic_type+"/"+hic_r[i]+"\n      filetype: fastqsanger.gz"
    with open(path_script+"/templates/wf4_run.sample.yaml", 'r') as sample_file:
        filedata = sample_file.read()
    pattern = r'\["(.*)"\]' # Matches the fields to replace
    to_fill = re.findall(pattern, filedata)
    dic_data_ids['hic']=str_hic
    dic_data_ids['Species Name']=list_metadata[assembly_id]['Name']
    dic_data_ids['Assembly Name']=assembly_id

    # Set trimhic based on HiC type
    # Arima data needs trimming (true), dovetail/other doesn't (false)
    dic_data_ids['trimhic'] = 'true' if hic_type.lower() == 'arima' else 'false'

    for i in to_fill:
        filedata = filedata.replace('["'+i+'"]', dic_data_ids[i] )
    with open(list_metadata[assembly_id]['job_files']['Workflow_4'], 'w') as yaml_wf4:
        yaml_wf4.write(filedata)

def prepare_yaml_wf8(assembly_id, list_metadata, invocation_wf4, profile_data, haplotype_code):
    dic_data_ids=get_datasets_ids(invocation_wf4)
    path_script=profile_data['path_script']

    # Map haplotype code to GFA output name and haplotype value
    if haplotype_code == 'hap1':
        gfa_output_name = 'usable hap1 gfa'
        haplotype_value = 'Haplotype 1'
    elif haplotype_code == 'hap2':
        gfa_output_name = 'usable hap2 gfa'
        haplotype_value = 'Haplotype 2'
    else:
        raise ValueError(f"Unknown haplotype code: {haplotype_code}")

    # Map the old gfa_assembly name to the haplotype-specific output
    dic_data_ids['gfa_assembly'] = dic_data_ids.get(gfa_output_name, '')

    with open(path_script+"/templates/wf8_run_sample.yaml", 'r') as sample_file:
        filedata = sample_file.read()
    pattern = r'\["(.*)"\]' # Matches the fields to replace
    to_fill = re.findall(pattern, filedata)
    dic_data_ids['Species Name']=list_metadata[assembly_id]['Name']
    dic_data_ids['Assembly Name']=assembly_id
    dic_data_ids['haplotype']=haplotype_value
    for i in to_fill:
        filedata = filedata.replace('["'+i+'"]', dic_data_ids[i] )

    # Use the haplotype-specific job file key
    wf8_key = f"Workflow_8_{haplotype_code}"
    with open(list_metadata[assembly_id]['job_files'][wf8_key], 'w') as yaml_wf8:
        yaml_wf8.write(filedata)
        
def prepare_yaml_wf0(assembly_id, list_metadata, invocation_wf4, profile_data):
    dic_data_ids=get_datasets_ids(invocation_wf4)
    path_script=profile_data['path_script']

    # Check that email is provided (required for Workflow 0)
    if 'email' not in profile_data or not profile_data['email']:
        raise SystemExit("Error: 'email' field is required in the profile for Workflow 0 (mitochondrial assembly). Please add your email address to the profile YAML file.")

    with open(path_script+"/templates/wf0_run.sample.yaml", 'r') as sample_file:
        filedata = sample_file.read()
    pattern = r'\["(.*)"\]' # Matches the fields to replace
    to_fill = re.findall(pattern, filedata)
    dic_data_ids['Species Name']=list_metadata[assembly_id]['Name']
    dic_data_ids['Assembly Name']=assembly_id
    # Add Latin Name (species name with spaces instead of underscores)
    dic_data_ids['Latin Name']=list_metadata[assembly_id]['Name'].replace("_", " ")
    # Add email (required field)
    dic_data_ids['email'] = profile_data['email']
    for i in to_fill:
        filedata = filedata.replace('["'+i+'"]', dic_data_ids[i] )
    with open(list_metadata[assembly_id]['job_files']['Workflow_0'], 'w') as yaml_wf0:
        yaml_wf0.write(filedata)

def prepare_yaml_wf9(assembly_id, species_name, invocation_wf8, haplotype, output_file, template_file, taxon_ID=None):
    """
    Prepare YAML job file for workflow 9 (decontamination).

    Args:
        assembly_id (str): Assembly ID (e.g., 'bTaeGut2')
        species_name (str): Species name with underscores (e.g., 'Taeniopygia_guttata')
        invocation_wf8 (dict): Galaxy invocation object from workflow 8
        haplotype (str): Full haplotype name ('Haplotype 1', 'Haplotype 2', 'Maternal', 'Paternal')
        output_file (str): Path to output YAML file
        template_file (str): Path to template file (legacy or fcs)
        taxon_ID (str, optional): NCBI taxonomy ID (required for FCS version)
    """
    dic_data_ids = get_datasets_ids(invocation_wf8)

    # Add additional fields
    dic_data_ids['Species Name'] = species_name
    dic_data_ids['Assembly Name'] = assembly_id
    dic_data_ids['haplotype'] = haplotype

    # Add taxon_ID if provided (for FCS version)
    if taxon_ID is not None:
        dic_data_ids['taxon_ID'] = str(taxon_ID)

    # Read template and replace placeholders
    with open(template_file, 'r') as sample_file:
        filedata = sample_file.read()

    pattern = r'\["(.*)"\]'  # Matches the fields to replace
    to_fill = re.findall(pattern, filedata)

    for field in to_fill:
        if field in dic_data_ids:
            filedata = filedata.replace('["'+field+'"]', dic_data_ids[field])
        else:
            raise KeyError(f"Required field '{field}' not found in data. Available fields: {list(dic_data_ids.keys())}")

    # Write output YAML
    with open(output_file, 'w') as yaml_wf9:
        yaml_wf9.write(filedata)

def prepare_yaml_precuration(assembly_id, invocation_wf4, invocation_wf9_hap1, invocation_wf9_hap2, output_file, template_file):
    """
    Prepare YAML job file for pre-curation workflow (PretextMap generation).

    Args:
        assembly_id (str): Assembly ID (e.g., 'bTaeGut2')
        invocation_wf4 (dict): Galaxy invocation object from workflow 4
        invocation_wf9_hap1 (dict): Galaxy invocation object from workflow 9 haplotype 1
        invocation_wf9_hap2 (dict): Galaxy invocation object from workflow 9 haplotype 2
        output_file (str): Path to output YAML file
        template_file (str): Path to template file
    """
    # Get dataset IDs from workflow 4
    dic_data_ids_wf4 = get_datasets_ids(invocation_wf4)

    # Get dataset IDs from workflow 9 haplotypes
    dic_data_ids_wf9_hap1 = get_datasets_ids(invocation_wf9_hap1)
    dic_data_ids_wf9_hap2 = get_datasets_ids(invocation_wf9_hap2)

    # Build the data dictionary with required fields
    dic_data_ids = {}

    # Get inputs from WF4
    dic_data_ids['hifi'] = dic_data_ids_wf4.get('HiFi reads without adapters', '')
    dic_data_ids['hic'] = dic_data_ids_wf4.get('Trimmed Hi-C reads', '')

    # Get haplotypes from WF9
    dic_data_ids['hap_1'] = dic_data_ids_wf9_hap1.get('Final Decontaminated Assembly', '')
    dic_data_ids['hap_2'] = dic_data_ids_wf9_hap2.get('Final Decontaminated Assembly', '')

    # Set parameters
    dic_data_ids['sechap'] = 'true'  # Always use second haplotype
    dic_data_ids['h1suf'] = 'H1'     # First haplotype suffix
    dic_data_ids['h2suf'] = 'H2'     # Second haplotype suffix

    # Set trimhic to false - reads are already trimmed by WF4
    # Pre-curation uses "Trimmed Hi-C reads" output from WF4, which are already processed
    dic_data_ids['trimhic'] = 'false'

    # Read template and replace placeholders
    with open(template_file, 'r') as sample_file:
        filedata = sample_file.read()

    pattern = r'\["(.*)"\]'  # Matches the fields to replace
    to_fill = re.findall(pattern, filedata)

    for field in to_fill:
        if field in dic_data_ids:
            filedata = filedata.replace('["'+field+'"]', dic_data_ids[field])
        else:
            raise KeyError(f"Required field '{field}' not found in data. Available fields: {list(dic_data_ids.keys())}")

    # Write output YAML
    with open(output_file, 'w') as yaml_precuration:
        yaml_precuration.write(filedata)