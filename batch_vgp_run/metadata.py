#!/usr/bin/env python3

"""
Metadata management functions for VGP pipeline.

This module handles:
- Profile loading and validation (YAML configuration files)
- Metadata loading and saving (JSON files tracking workflow progress)
- Galaxy connection setup
- Invocation state tracking
- Per-species metadata persistence
"""

import os
import json
import yaml
import time
from datetime import datetime
from bioblend.galaxy import GalaxyInstance
from batch_vgp_run.utils import fix_parameters
from batch_vgp_run.logging_utils import log_info, log_warning


def load_profile(profile_path):
    """Load and validate profile YAML file.

    Returns:
        dict: Profile data with Galaxy_instance, Galaxy_key, and workflow specifications
    """
    if not os.path.isfile(profile_path):
        raise SystemExit(f"Error: Profile file not found: {profile_path}")

    with open(profile_path, "r") as file:
        profile_data = yaml.safe_load(file)

    # Validate required fields
    if 'Galaxy_instance' not in profile_data:
        raise SystemExit("Error: Profile file missing 'Galaxy_instance' field")
    if 'Galaxy_key' not in profile_data:
        raise SystemExit("Error: Profile file missing 'Galaxy_key' field")

    return profile_data




def load_metadata(metadata_dir, suffix_run=''):
    """Load metadata from JSON files.

    Args:
        metadata_dir (str): Path to metadata directory
        suffix_run (str): Optional suffix for run-specific metadata files

    Returns:
        tuple: (list_metadata, dico_workflows) or (list_metadata, None) if workflow metadata doesn't exist
    """
    metadata_file = f"{metadata_dir}metadata_run{suffix_run}.json"
    workflow_metadata_file = f"{metadata_dir}metadata_workflow{suffix_run}.json"

    # Load main metadata
    if os.path.isfile(metadata_file):
        with open(metadata_file, "r") as json_file:
            list_metadata = json.load(json_file)

        # Load per-species metadata files (more recent)
        for species_id in list(list_metadata.keys()):
            species_metadata_file = f"{metadata_dir}metadata_{species_id}_run{suffix_run}.json"
            if os.path.isfile(species_metadata_file):
                try:
                    with open(species_metadata_file, "r") as json_file:
                        species_metadata = json.load(json_file)
                    list_metadata[species_id] = species_metadata
                except Exception as e:
                    log_warning(f"Could not load per-species metadata for {species_id}: {e}")
    else:
        list_metadata = {}

    # Load workflow metadata
    if os.path.isfile(workflow_metadata_file):
        with open(workflow_metadata_file, "r") as json_file:
            dico_workflows = json.load(json_file)
    else:
        dico_workflows = None

    return list_metadata, dico_workflows




def save_metadata(metadata_dir, list_metadata, suffix_run='', dico_workflows=None):
    """Save metadata to JSON files.

    Args:
        metadata_dir (str): Path to metadata directory
        list_metadata (dict): Species metadata dictionary
        suffix_run (str): Optional suffix for run-specific metadata files
        dico_workflows (dict): Optional workflow metadata dictionary
    """
    metadata_file = f"{metadata_dir}metadata_run{suffix_run}.json"

    with open(metadata_file, "w") as json_file:
        json.dump(list_metadata, json_file, indent=4)

    if dico_workflows is not None:
        workflow_metadata_file = f"{metadata_dir}metadata_workflow{suffix_run}.json"
        with open(workflow_metadata_file, "w") as json_file:
            json.dump(dico_workflows, json_file, indent=4)




def setup_galaxy_connection(profile_data):
    """Setup Galaxy connection from profile data.

    Args:
        profile_data (dict): Profile dictionary with Galaxy_instance and Galaxy_key

    Returns:
        tuple: (gi, galaxy_instance) - GalaxyInstance object and normalized URL
    """
    galaxy_instance = profile_data['Galaxy_instance']
    api_key = profile_data['Galaxy_key']

    # Normalize Galaxy URL
    _, galaxy_instance = fix_parameters("", galaxy_instance)

    # Connect to Galaxy
    gi = GalaxyInstance(galaxy_instance, api_key)

    return gi, galaxy_instance




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



