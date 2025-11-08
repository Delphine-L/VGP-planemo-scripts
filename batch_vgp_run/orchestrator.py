#!/usr/bin/env python3

"""
Batch processing orchestration for VGP pipeline.

This module coordinates the execution of workflows across multiple species:
- Parallel species processing with threading
- Workflow dependency management
- Stateless, resumable execution model
- Invocation tracking and error handling
"""

import os
import json
import time
import threading
import subprocess
from datetime import datetime
from bioblend.galaxy import GalaxyInstance
from batch_vgp_run.galaxy_client import (
    get_or_find_history_id,
    check_invocation_complete,
    check_mitohifi_failure,
    check_required_outputs_exist,
    poll_until_invocation_complete,
    poll_until_outputs_ready,
    build_invocation_cache,
    fetch_invocation_from_history,
    get_datasets_ids
)
from batch_vgp_run.metadata import save_species_metadata, mark_invocation_as_failed
from batch_vgp_run.workflow_prep import (
    prepare_yaml_wf4,
    prepare_yaml_wf8,
    prepare_yaml_wf0,
    prepare_yaml_wf9,
    prepare_yaml_precuration
)
from batch_vgp_run.logging_utils import log_info, log_warning, log_error


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
            command_lines[key]="planemo run "+workflow_path+" "+job_yaml+" --engine external_galaxy --galaxy_url "+galaxy_instance+" --simultaneous_uploads --check_uploads_ok --galaxy_user_key "+galaxy_key+" --history_id "+history_id+" --test_output_json "+res_file+" > "+log_file+" 2>&1"
        else:
            command_lines[key]="planemo run "+workflow_path+" "+job_yaml+" --engine external_galaxy --galaxy_url "+galaxy_instance+" --simultaneous_uploads --check_uploads_ok --galaxy_user_key "+galaxy_key+" --history_name "+history_name+" --test_output_json "+res_file+" > "+log_file+" 2>&1"

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

        # Run planemo and check return code
        return_code = os.system(command_lines['Workflow_1'])
        log_file = list_metadata[assembly_id]['planemo_logs']['Workflow_1']

        if return_code != 0:
            print(f"ERROR: Workflow 1 for {assembly_id} failed with return code {return_code}")
            print(f"Check log file: {log_file}")
            mark_invocation_as_failed(
                assembly_id,
                list_metadata,
                'Workflow_1',
                'NA',  # No invocation ID when planemo fails
                profile_data,
                suffix_run
            )
            return {assembly_id: list_metadata[assembly_id]}

        # Even if return code is 0, check log for errors (planemo sometimes exits 0 despite internal errors)
        if os.path.exists(log_file):
            try:
                with open(log_file, 'r') as f:
                    log_content = f.read()
                    if 'AssertionError' in log_content or 'Error' in log_content or 'Failed' in log_content:
                        error_lines = [line for line in log_content.split('\n') if 'AssertionError' in line or 'Error:' in line or 'ERROR:' in line]
                        if error_lines:
                            print(f"ERROR: Workflow 1 for {assembly_id} encountered errors despite return code 0:")
                            for line in error_lines[:5]:
                                print(f"  {line}")
                            print(f"Check full log file: {log_file}")
                            mark_invocation_as_failed(
                                assembly_id,
                                list_metadata,
                                'Workflow_1',
                                'NA',
                                profile_data,
                                suffix_run
                            )
                            return {assembly_id: list_metadata[assembly_id]}
            except Exception as e:
                print(f"Warning: Could not read log file for error checking: {e}")

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

        # Run planemo and check return code
        return_code = os.system(command_lines["Workflow_4"])
        log_file = list_metadata[assembly_id]['planemo_logs']['Workflow_4']

        if return_code != 0:
            print(f"ERROR: Workflow 4 for {assembly_id} failed with return code {return_code}")
            print(f"Check log file: {log_file}")
            mark_invocation_as_failed(
                assembly_id,
                list_metadata,
                'Workflow_4',
                'NA',  # No invocation ID when planemo fails
                profile_data,
                suffix_run
            )
            return {assembly_id: list_metadata[assembly_id]}

        # Even if return code is 0, check log for errors (planemo sometimes exits 0 despite internal errors)
        if os.path.exists(log_file):
            try:
                with open(log_file, 'r') as f:
                    log_content = f.read()
                    if 'AssertionError' in log_content or 'Error' in log_content or 'Failed' in log_content:
                        # Check if these are actual errors (not just warnings)
                        error_lines = [line for line in log_content.split('\n') if 'AssertionError' in line or 'Error:' in line or 'ERROR:' in line]
                        if error_lines:
                            print(f"ERROR: Workflow 4 for {assembly_id} encountered errors despite return code 0:")
                            for line in error_lines[:5]:  # Show first 5 error lines
                                print(f"  {line}")
                            print(f"Check full log file: {log_file}")
                            mark_invocation_as_failed(
                                assembly_id,
                                list_metadata,
                                'Workflow_4',
                                'NA',
                                profile_data,
                                suffix_run
                            )
                            return {assembly_id: list_metadata[assembly_id]}
            except Exception as e:
                print(f"Warning: Could not read log file for error checking: {e}")

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
        # When resuming, check if JSON exists from previous background launch
        if is_resume:
            wf0_json_path = list_metadata[assembly_id]["invocation_jsons"]["Workflow_0"]
            if os.path.exists(wf0_json_path):
                try:
                    with open(wf0_json_path) as wf0json:
                        reswf0 = json.load(wf0json)
                    invocation_wf0 = reswf0["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                    list_metadata[assembly_id]["invocations"]["Workflow_0"] = invocation_wf0
                    print(f"Workflow 0 invocation found in JSON file: {invocation_wf0}\n")
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"Warning: Could not parse Workflow 0 JSON file: {e}")
                    print(f"Will re-launch Workflow 0...")
                    invocation_wf0 = None
            else:
                print(f"Workflow 0 JSON not found. Workflow may still be initializing from previous run.")
                print(f"Will check again on next resume.\n")
                invocation_wf0 = None

        # Launch if still not found
        if not invocation_wf0 or invocation_wf0 == 'NA':
            if is_resume:
                print(f"No previous run found for Workflow 0. Preparing and launching...")
            else:
                print(f"Preparing and launching Workflow 0 for {assembly_id}...")
            prepare_yaml_wf0(assembly_id, list_metadata, wf4_inv, profile_data)
            os.system(command_lines["Workflow_0"] + " &")
            print(f"Workflow 0 for {assembly_id} ({species_name}) has been launched in background.\n")
            # Don't wait for JSON since it's running in background

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
    # First, load per-haplotype metadata if resuming (crash recovery)
    if is_resume:
        for hap_code in ['hap1', 'hap2']:
            wf8_key = f"Workflow_8_{hap_code}"
            hap_metadata_file = f"{profile_data['Metadata_directory']}metadata_{assembly_id}_{wf8_key}_run{suffix_run}.json"

            if os.path.exists(hap_metadata_file):
                try:
                    with open(hap_metadata_file, 'r') as json_file:
                        hap_metadata = json.load(json_file)

                    invocation_id = hap_metadata.get('invocation_id')
                    if invocation_id and list_metadata[assembly_id]["invocations"].get(wf8_key, 'NA') == 'NA':
                        # Restore invocation from per-haplotype metadata
                        list_metadata[assembly_id]["invocations"][wf8_key] = invocation_id
                        print(f"Restored {wf8_key} invocation from per-haplotype metadata: {invocation_id}")
                except Exception as e:
                    print(f"Warning: Could not load per-haplotype metadata for {wf8_key}: {e}")

    # Check if WF8 invocations already exist (don't need to check WF4 if so)

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

    # Only check WF4 status if we need to launch WF8 (saves time if WF8 already exists)
    if wf8_to_launch:
        # For --resume mode: Check if WF4 is complete and outputs are ready before launching WF8
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

    # Prepare YAML for workflows that need to be launched
    if wf8_to_launch:
        for hap_code in wf8_to_launch:
            prepare_yaml_wf8(assembly_id, list_metadata, wf4_inv, profile_data, hap_code)

    # Launch workflows in parallel using threads (each thread waits for planemo to complete)
    if wf8_to_launch:
        print(f"Launching Workflow 8 for {assembly_id} ({len(wf8_to_launch)} haplotypes in parallel)...")

        # Create threads for each haplotype
        wf8_threads = []
        wf8_thread_results = {}  # Store results from each thread

        def launch_wf8_haplotype(hap_code, haplotype_name, wf8_key):
            """Thread function to launch a single WF8 haplotype"""
            result = {'success': False, 'invocation_id': None}

            if wf8_key in command_lines:
                print(f"  Starting Workflow 8 ({haplotype_name}) in thread...")
                # Launch WITHOUT & - this blocks until planemo finishes
                return_code = os.system(command_lines[wf8_key])
                log_file = list_metadata[assembly_id]['planemo_logs'].get(wf8_key)

                if return_code != 0:
                    print(f"  ERROR: Workflow 8 ({haplotype_name}) failed with return code {return_code}")
                    print(f"  Check log file: {log_file}")
                    mark_invocation_as_failed(
                        assembly_id,
                        list_metadata,
                        wf8_key,
                        'NA',  # No invocation ID when planemo fails
                        profile_data,
                        suffix_run
                    )
                    return

                # Even if return code is 0, check log for errors
                if log_file and os.path.exists(log_file):
                    try:
                        with open(log_file, 'r') as f:
                            log_content = f.read()
                            if 'AssertionError' in log_content or 'Error' in log_content or 'Failed' in log_content:
                                error_lines = [line for line in log_content.split('\n') if 'AssertionError' in line or 'Error:' in line or 'ERROR:' in line]
                                if error_lines:
                                    print(f"  ERROR: Workflow 8 ({haplotype_name}) encountered errors despite return code 0:")
                                    for line in error_lines[:5]:
                                        print(f"    {line}")
                                    print(f"  Check full log file: {log_file}")
                                    mark_invocation_as_failed(
                                        assembly_id,
                                        list_metadata,
                                        wf8_key,
                                        'NA',
                                        profile_data,
                                        suffix_run
                                    )
                                    return
                    except Exception as e:
                        print(f"  Warning: Could not read log file for error checking: {e}")

                print(f"  ✓ Workflow 8 ({haplotype_name}) planemo command completed")

                # Immediately read JSON and save per-haplotype metadata
                wf8_json_path = list_metadata[assembly_id]["invocation_jsons"].get(wf8_key)
                if wf8_json_path and os.path.exists(wf8_json_path):
                    try:
                        with open(wf8_json_path) as wf8json:
                            reswf8 = json.load(wf8json)
                        invocation_id = reswf8["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                        result['invocation_id'] = invocation_id
                        result['success'] = True

                        # Save per-haplotype metadata immediately
                        haplotype_metadata = {
                            'invocation_id': invocation_id,
                            'workflow_key': wf8_key,
                            'timestamp': datetime.now().isoformat(),
                            'state': 'launched'
                        }
                        hap_metadata_file = f"{profile_data['Metadata_directory']}metadata_{assembly_id}_{wf8_key}_run{suffix_run}.json"
                        with open(hap_metadata_file, 'w') as json_file:
                            json.dump(haplotype_metadata, json_file, indent=4)
                        print(f"  ✓ Saved per-haplotype metadata: {hap_metadata_file}")

                    except (json.JSONDecodeError, KeyError, IOError) as e:
                        print(f"  ⚠ Warning: Could not process Workflow 8 ({haplotype_name}) result: {e}")
                else:
                    print(f"  ⚠ Warning: JSON file not found for Workflow 8 ({haplotype_name}): {wf8_json_path}")
            else:
                print(f"⚠ ERROR: Command line for {wf8_key} not found in command_lines dictionary!")
                print(f"  Available keys: {list(command_lines.keys())}")

            wf8_thread_results[hap_code] = result

        # Start all haplotype threads
        for hap_code in wf8_to_launch:
            haplotype_name = hap_mapping[hap_code]
            wf8_key = f"Workflow_8_{hap_code}"

            thread = threading.Thread(
                target=launch_wf8_haplotype,
                args=(hap_code, haplotype_name, wf8_key),
                daemon=False
            )
            thread.start()
            wf8_threads.append(thread)

        # Wait for all threads to complete
        print(f"  Waiting for all Workflow 8 haplotypes to complete...")
        for thread in wf8_threads:
            thread.join()

        print(f"✓ All Workflow 8 haplotypes completed for {assembly_id}\n")

        # Merge thread results into main metadata
        print(f"Merging haplotype results into species metadata...")
        for hap_code in wf8_to_launch:
            result = wf8_thread_results.get(hap_code, {})
            if result.get('success') and result.get('invocation_id'):
                wf8_key = f"Workflow_8_{hap_code}"
                invocation_id = result['invocation_id']
                list_metadata[assembly_id]["invocations"][wf8_key] = invocation_id
                wf8_invocations[hap_code] = invocation_id
                print(f"✓ Merged Workflow 8 ({hap_mapping[hap_code]}): {invocation_id}")
            else:
                print(f"⚠ Warning: Workflow 8 ({hap_mapping[hap_code]}) did not complete successfully")

        print(f"Workflow 8 processing complete for {assembly_id}\n")

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

    # First, load per-haplotype metadata if resuming (crash recovery)
    if is_resume:
        for hap_code in ['hap1', 'hap2']:
            wf9_key = f"Workflow_9_{hap_code}"
            hap_metadata_file = f"{profile_data['Metadata_directory']}metadata_{assembly_id}_{wf9_key}_run{suffix_run}.json"

            if os.path.exists(hap_metadata_file):
                try:
                    with open(hap_metadata_file, 'r') as json_file:
                        hap_metadata = json.load(json_file)

                    invocation_id = hap_metadata.get('invocation_id')
                    if invocation_id and list_metadata[assembly_id]["invocations"].get(wf9_key, 'NA') == 'NA':
                        # Restore invocation from per-haplotype metadata
                        list_metadata[assembly_id]["invocations"][wf9_key] = invocation_id
                        print(f"Restored {wf9_key} invocation from per-haplotype metadata: {invocation_id}")
                except Exception as e:
                    print(f"Warning: Could not load per-haplotype metadata for {wf9_key}: {e}")

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
                # First check if datasets command is available in PATH
                datasets_path = shutil.which('datasets')

                # If not in PATH, check common installation locations
                if not datasets_path:
                    common_locations = [
                        os.path.expanduser('~/.local/bin/datasets'),
                        '/usr/local/bin/datasets',
                        '/usr/bin/datasets',
                        os.path.join(os.environ.get('HOME', ''), '.local/bin/datasets'),
                    ]
                    for location in common_locations:
                        if os.path.isfile(location) and os.access(location, os.X_OK):
                            datasets_path = location
                            print(f"Found datasets at {datasets_path} (not in PATH, but found in common location)")
                            break

                # If still not found, show detailed error
                if not datasets_path:
                    print(f"Error: 'datasets' command not found in PATH or common locations")
                    print(f"Current PATH: {os.environ.get('PATH', 'Not set')}")
                    print(f"Searched locations: {', '.join(common_locations)}")
                    print(f"")
                    print(f"To fix this issue:")
                    print(f"  1. If running via SLURM, add 'source ~/.bashrc' to your job script")
                    print(f"  2. Or add 'export PATH=\"$HOME/.local/bin:$PATH\"' to your job script")
                    print(f"  3. Or install datasets: bash installs.sh")
                    print(f"")
                    print(f"More info: https://www.ncbi.nlm.nih.gov/datasets/docs/v2/command-line-tools/download-and-install/")
                    raise SystemExit(f"Failed to get taxon ID for {species_name}: datasets tool not found")

                # Check if it's executable
                if not os.access(datasets_path, os.X_OK):
                    print(f"Error: 'datasets' found at {datasets_path} but is not executable")
                    print(f"Run: chmod +x {datasets_path}")
                    raise SystemExit(f"Failed to get taxon ID for {species_name}: datasets tool not executable")

                # Use the full path to ensure it works even if PATH is not set correctly
                datasets_command = [datasets_path, 'summary', 'taxonomy', 'taxon', species_name_for_ncbi, '--as-json-lines']
                data_type = subprocess.run(datasets_command, capture_output=True, text=True, check=True)
                taxon_data = json.loads(data_type.stdout)
                taxon_id = str(taxon_data['taxonomy']['tax_id'])
                taxon_name = taxon_data['taxonomy']['current_scientific_name']['name']
                print(f"Retrieved taxon ID {taxon_id} for {species_name} ({taxon_name})")

                # Store in metadata for future runs
                list_metadata[assembly_id]['taxon_id'] = taxon_id
            except FileNotFoundError as e:
                print(f"Error: 'datasets' command not found: {e}")
                print(f"Please install the NCBI datasets tool:")
                print(f"  1. Run: bash installs.sh")
                print(f"  2. Or manually download from: https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/")
                print(f"  3. Add to PATH: export PATH=\"$HOME/.local/bin:$PATH\"")
                raise SystemExit(f"Failed to get taxon ID for {species_name}: datasets tool not found")
            except PermissionError as e:
                datasets_path = shutil.which('datasets') or 'datasets'
                print(f"Error: Permission denied when trying to execute 'datasets': {e}")
                print(f"Dataset binary location: {datasets_path}")
                print(f"Possible fixes:")
                print(f"  1. Check execute permissions: ls -la {datasets_path}")
                print(f"  2. Add execute permission: chmod +x {datasets_path}")
                print(f"  3. Check if filesystem is mounted with noexec: mount | grep $(df {datasets_path} | tail -1 | awk '{{print $1}}')")
                print(f"  4. Try installing to a different location with exec permissions")
                raise SystemExit(f"Failed to get taxon ID for {species_name}: permission denied")
            except subprocess.CalledProcessError as e:
                print(f"Error running datasets command: {e}")
                print(f"Command: {' '.join(datasets_command)}")
                print(f"Return code: {e.returncode}")
                print(f"Stderr: {e.stderr}")
                raise SystemExit(f"Failed to get taxon ID for {species_name}")
            except Exception as e:
                print(f"Error querying NCBI dataset for {species_name}: {e}")
                print(f"Error type: {type(e).__name__}")
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

    # Launch workflows in parallel using threads (each thread waits for planemo to complete)
    if wf9_to_launch:
        print(f"Launching Workflow 9 for {assembly_id} ({len(wf9_to_launch)} haplotypes in parallel)...")

        # Create threads for each haplotype
        wf9_threads = []
        wf9_thread_results = {}  # Store results from each thread

        def launch_wf9_haplotype(hap_code, haplotype_name, wf9_key):
            """Thread function to launch a single WF9 haplotype"""
            result = {'success': False, 'invocation_id': None}

            if wf9_key in command_lines:
                print(f"  Starting Workflow 9 ({haplotype_name}) in thread...")
                # Launch WITHOUT & - this blocks until planemo finishes
                return_code = os.system(command_lines[wf9_key])
                log_file = list_metadata[assembly_id]['planemo_logs'].get(wf9_key)

                if return_code != 0:
                    print(f"  ERROR: Workflow 9 ({haplotype_name}) failed with return code {return_code}")
                    print(f"  Check log file: {log_file}")
                    mark_invocation_as_failed(
                        assembly_id,
                        list_metadata,
                        wf9_key,
                        'NA',  # No invocation ID when planemo fails
                        profile_data,
                        suffix_run
                    )
                    return

                # Even if return code is 0, check log for errors
                if log_file and os.path.exists(log_file):
                    try:
                        with open(log_file, 'r') as f:
                            log_content = f.read()
                            if 'AssertionError' in log_content or 'Error' in log_content or 'Failed' in log_content:
                                error_lines = [line for line in log_content.split('\n') if 'AssertionError' in line or 'Error:' in line or 'ERROR:' in line]
                                if error_lines:
                                    print(f"  ERROR: Workflow 9 ({haplotype_name}) encountered errors despite return code 0:")
                                    for line in error_lines[:5]:
                                        print(f"    {line}")
                                    print(f"  Check full log file: {log_file}")
                                    mark_invocation_as_failed(
                                        assembly_id,
                                        list_metadata,
                                        wf9_key,
                                        'NA',
                                        profile_data,
                                        suffix_run
                                    )
                                    return
                    except Exception as e:
                        print(f"  Warning: Could not read log file for error checking: {e}")

                print(f"  ✓ Workflow 9 ({haplotype_name}) planemo command completed")

                # Immediately read JSON and save per-haplotype metadata
                wf9_json_path = list_metadata[assembly_id]["invocation_jsons"].get(wf9_key)
                if wf9_json_path and os.path.exists(wf9_json_path):
                    try:
                        with open(wf9_json_path) as wf9json:
                            reswf9 = json.load(wf9json)
                        invocation_id = reswf9["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                        result['invocation_id'] = invocation_id
                        result['success'] = True

                        # Save per-haplotype metadata immediately
                        haplotype_metadata = {
                            'invocation_id': invocation_id,
                            'workflow_key': wf9_key,
                            'timestamp': datetime.now().isoformat(),
                            'state': 'launched'
                        }
                        hap_metadata_file = f"{profile_data['Metadata_directory']}metadata_{assembly_id}_{wf9_key}_run{suffix_run}.json"
                        with open(hap_metadata_file, 'w') as json_file:
                            json.dump(haplotype_metadata, json_file, indent=4)
                        print(f"  ✓ Saved per-haplotype metadata: {hap_metadata_file}")

                    except (json.JSONDecodeError, KeyError, IOError) as e:
                        print(f"  ⚠ Warning: Could not process Workflow 9 ({haplotype_name}) result: {e}")
                else:
                    print(f"  ⚠ Warning: JSON file not found for Workflow 9 ({haplotype_name}): {wf9_json_path}")
            else:
                print(f"⚠ ERROR: Command line for {wf9_key} not found in command_lines dictionary!")
                print(f"  Available keys: {list(command_lines.keys())}")

            wf9_thread_results[hap_code] = result

        # Start all haplotype threads
        for hap_code in wf9_to_launch:
            haplotype_name = hap_mapping[hap_code]
            wf9_key = f"Workflow_9_{hap_code}"

            thread = threading.Thread(
                target=launch_wf9_haplotype,
                args=(hap_code, haplotype_name, wf9_key),
                daemon=False
            )
            thread.start()
            wf9_threads.append(thread)

        # Wait for all threads to complete
        print(f"  Waiting for all Workflow 9 haplotypes to complete...")
        for thread in wf9_threads:
            thread.join()

        print(f"✓ All Workflow 9 haplotypes completed for {assembly_id}\n")

        # Merge thread results into main metadata
        print(f"Merging haplotype results into species metadata...")
        for hap_code in wf9_to_launch:
            result = wf9_thread_results.get(hap_code, {})
            if result.get('success') and result.get('invocation_id'):
                wf9_key = f"Workflow_9_{hap_code}"
                invocation_id = result['invocation_id']
                list_metadata[assembly_id]["invocations"][wf9_key] = invocation_id
                wf9_invocations[hap_code] = invocation_id
                print(f"✓ Merged Workflow 9 ({hap_mapping[hap_code]}): {invocation_id}")
            else:
                print(f"⚠ Warning: Workflow 9 ({hap_mapping[hap_code]}) did not complete successfully")

        print(f"Workflow 9 processing complete for {assembly_id}\n")

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
                return_code = os.system(command_lines["Workflow_PreCuration"])

                if return_code != 0:
                    print(f"ERROR: Pre-curation workflow for {assembly_id} failed with return code {return_code}")
                    print(f"Check log file: {list_metadata[assembly_id]['planemo_logs']['Workflow_PreCuration']}")
                    mark_invocation_as_failed(
                        assembly_id,
                        list_metadata,
                        'Workflow_PreCuration',
                        'NA',  # No invocation ID when planemo fails
                        profile_data,
                        suffix_run
                    )
                else:
                    # Get invocation ID from completed JSON
                    try:
                        precuration_json = open(list_metadata[assembly_id]["invocation_jsons"]["Workflow_PreCuration"])
                        res_precuration = json.load(precuration_json)
                        invocation_precuration = res_precuration["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                        list_metadata[assembly_id]["invocations"]["Workflow_PreCuration"] = invocation_precuration
                        print(f"Pre-curation workflow launched: {invocation_precuration}\n")
                    except (json.JSONDecodeError, KeyError, IOError) as e:
                        print(f"Warning: Could not parse PreCuration JSON: {e}")

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


