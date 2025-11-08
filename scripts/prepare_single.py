#!/usr/bin/env python3

"""
Unified workflow preparation script for VGP assembly pipelines.
Replaces individual prepare_wf*.py scripts with a single tool.

Usage:
    prepare_single.py --workflow 1 -t tracking_table.tsv -g https://usegalaxy.org/ -k $API_KEY [OPTIONS]
"""

import argparse
import pandas
import os
import re
import pathlib
import textwrap
import json
import yaml
import shutil
import sys
from bioblend.galaxy import GalaxyInstance

# Ensure repo root is in path for both batch_vgp_run and scripts imports
script_dir = pathlib.Path(__file__).parent.resolve()
repo_root = script_dir.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

# Import from organized modules
from batch_vgp_run import utils
from batch_vgp_run import metadata
from batch_vgp_run import galaxy_client
from batch_vgp_run import workflow_manager

# Import get_urls from scripts package
try:
    # When installed as package
    from scripts.get_urls import get_urls
except ImportError:
    # When running directly - should work now that repo_root is in path
    from scripts.get_urls import get_urls


# Workflow configurations
WORKFLOW_CONFIGS = {
    "1": {
        "name": "kmer-profiling-hifi-VGP1",
        "label": "VGP1",
        "template": "wf1_run.sample.yaml",
        "requires": None,
        "invocation_column": None,
        "result_column": None,
        "description": "Kmer profiling with HiFi data",
        "output_columns": ["Job_File_wf1", "Results_wf1", "Command_wf1", "Invocation_wf1"]
    },
    "0": {
        "name": "Mitogenome-assembly-VGP0",
        "label": "VGP0",
        "template": "wf0_run.sample.yaml",
        "requires": "4",
        "invocation_column": "Invocation_wf4",
        "result_column": "Results_wf4",
        "description": "Mitochondrial genome assembly",
        "output_columns": ["Job_File_wf0", "Results_wf0", "Command_wf0", "Invocation_wf0", "History_id"]
    },
    "4": {
        "name": "Assembly-Hifi-HiC-phasing-VGP4",
        "label": "VGP4",
        "template": "wf4_run.sample.yaml",
        "requires": "1",
        "invocation_column": "Invocation_wf1",
        "result_column": "Results_wf1",
        "description": "Assembly with HiFi and Hi-C phasing",
        "output_columns": ["Job_File_wf4", "Results_wf4", "Command_wf4", "Invocation_wf4", "History_id", "Wf1_Report"]
    },
    "8": {
        "name": "Scaffolding-HiC-VGP8",
        "label": "VGP8",
        "template": "wf8_run_sample.yaml",
        "requires": "4",
        "invocation_column": "Invocation_wf4",
        "result_column": "Results_wf4",
        "description": "Haplotype-specific assembly",
        "has_haplotypes": True,
        "output_columns": ["Job_File_wf8_{hap}", "Results_wf8_{hap}", "Command_wf8_{hap}", "Invocation_wf8_{hap}", "History_id"]
    },
    "9": {
        "name": "Assembly-decontamination-VGP9",
        "label": "VGP9",
        "template_fcs": "wf9_run_sample_fcs.yaml",
        "template_legacy": "wf9_run_sample_legacy.yaml",
        "requires": "8",
        "invocation_column": "Invocation_wf8_{hap}",
        "result_column": "Results_wf8_{hap}",
        "description": "Decontamination (NCBI FCS-GX or Kraken2 legacy)",
        "has_haplotypes": True,
        "has_modes": True,
        "output_columns": ["Job_File_wf9_{hap}", "Results_wf9_{hap}", "Command_wf9_{hap}", "Invocation_wf9_{hap}"]
    },
    "precuration": {
        "name": "PretextMap-Generation",
        "label": "PreCuration",
        "template": "precuration_run.sample.yaml",
        "requires": ["4", "9"],  # Requires WF4 and both WF9 haplotypes
        "invocation_column": ["Invocation_wf4", "Invocation_wf9_hap1", "Invocation_wf9_hap2"],
        "result_column": ["Results_wf4", "Results_wf9_hap1", "Results_wf9_hap2"],
        "description": "Pre-curation PretextMap generation",
        "output_columns": ["Job_File_precuration", "Results_precuration", "Command_precuration", "Invocation_precuration"]
    }
}


def is_valid_invocation(value):
    """
    Check if a value represents a valid invocation ID.

    Returns False if the value is:
    - pandas NaN (from reading TSV with NA values)
    - String "NA" or "na"
    - String "nan" (from str(NaN))
    - None
    - Empty string

    Args:
        value: The value to check (any type)

    Returns:
        bool: True if valid invocation ID, False otherwise
    """
    # Check for pandas NaN
    if pandas.isna(value):
        return False

    # Convert to string for other checks
    str_value = str(value).strip().lower()

    # Check for various NA representations
    if str_value in ['na', 'nan', 'none', '']:
        return False

    return True


def check_metadata_for_workflow(spec_id, workflow_key, list_metadata, gi):
    """
    Check if workflow has already been prepared or run.

    Args:
        spec_id (str): Species/assembly ID
        workflow_key (str): Workflow key in metadata (e.g., "Workflow_4", "Workflow_8_hap1")
        list_metadata (dict): Metadata dictionary
        gi (GalaxyInstance): Galaxy instance for fetching missing data

    Returns:
        tuple: (skip_generation, invocation_id, dataset_ids)
            - skip_generation (bool): True if we should skip generating new command
            - invocation_id (str or None): Invocation ID if found
            - dataset_ids (dict or None): Dataset IDs if available
    """
    if not list_metadata or spec_id not in list_metadata:
        return False, None, None

    species_metadata = list_metadata[spec_id]

    # Check if job file path exists in metadata
    job_file = species_metadata.get("job_files", {}).get(workflow_key)
    if job_file and job_file != "NA":
        # Verify the job file actually exists on disk
        if not os.path.exists(job_file):
            print(f"  {spec_id}: {workflow_key} job file in metadata but not found on disk ({job_file}), regenerating...")
            return False, None, None

        # Job file exists on disk - check if we have invocation/datasets
        invocation_id = species_metadata.get("invocations", {}).get(workflow_key, "NA")
        dataset_ids = species_metadata.get("dataset_ids", {}).get(workflow_key)

        if invocation_id != "NA" and dataset_ids:
            # Have job file + invocation + datasets - skip generation
            print(f"  {spec_id}: {workflow_key} already complete in metadata (invocation: {invocation_id})")
            return True, invocation_id, dataset_ids
        elif invocation_id != "NA" and not dataset_ids:
            # Have job file + invocation but no datasets - fetch from Galaxy
            print(f"  {spec_id}: {workflow_key} invocation {invocation_id} found in metadata, fetching datasets...")
            try:
                inv_details = gi.invocations.show_invocation(invocation_id)
                fetched_dataset_ids = galaxy_client.get_datasets_ids(inv_details)

                # Update metadata with fetched datasets
                if "dataset_ids" not in species_metadata:
                    species_metadata["dataset_ids"] = {}
                species_metadata["dataset_ids"][workflow_key] = fetched_dataset_ids

                print(f"  {spec_id}: Fetched {len(fetched_dataset_ids)} dataset IDs from Galaxy")
                return True, invocation_id, fetched_dataset_ids
            except Exception as e:
                print(f"  {spec_id}: Warning - could not fetch datasets for invocation {invocation_id}: {e}")
                # Still skip generation since job file exists on disk
                print(f"  {spec_id}: {workflow_key} job file exists on disk, skipping generation")
                return True, invocation_id, None
        else:
            # Have job file but no invocation yet (workflow prepared but not run)
            print(f"  {spec_id}: {workflow_key} job file exists on disk, skipping generation")
            return True, None, None

    # No job file in metadata - check if we have invocation anyway (shouldn't happen but handle it)
    invocation_id = species_metadata.get("invocations", {}).get(workflow_key, "NA")
    if invocation_id != "NA" and invocation_id:
        # Have invocation but no job file recorded - unusual case
        dataset_ids = species_metadata.get("dataset_ids", {}).get(workflow_key)
        if dataset_ids:
            print(f"  {spec_id}: {workflow_key} invocation found with datasets but no job file")
            return True, invocation_id, dataset_ids
        else:
            # Fetch datasets
            print(f"  {spec_id}: {workflow_key} invocation {invocation_id} found, fetching datasets...")
            try:
                inv_details = gi.invocations.show_invocation(invocation_id)
                fetched_dataset_ids = galaxy_client.get_datasets_ids(inv_details)

                # Update metadata with fetched datasets
                if "dataset_ids" not in species_metadata:
                    species_metadata["dataset_ids"] = {}
                species_metadata["dataset_ids"][workflow_key] = fetched_dataset_ids

                print(f"  {spec_id}: Fetched {len(fetched_dataset_ids)} dataset IDs from Galaxy")
                return True, invocation_id, fetched_dataset_ids
            except Exception as e:
                print(f"  {spec_id}: Warning - could not fetch datasets for invocation {invocation_id}: {e}")
                return False, invocation_id, None

    # No job file and no invocation - generate new command
    return False, None, None


def prepare_workflow_1(infos, gi, workflow_path, galaxy_instance, suffix_run, path_script, list_metadata=None):
    """Prepare Workflow 1 (Kmer profiling with HiFi)"""
    suffix_run = utils.normalize_suffix(suffix_run)
    commands = []
    workflow_key = "Workflow_1"

    for i, row in infos.iterrows():
        spec_name = str(infos.iloc[i]['Species']).strip()
        assembly_id = str(infos.iloc[i]['Assembly']).strip()
        spec_id = utils.get_working_assembly(row, infos, i)

        # Check metadata first - skip if already prepared or complete
        skip_generation, metadata_invocation, metadata_datasets = check_metadata_for_workflow(
            spec_id, workflow_key, list_metadata, gi
        )

        if skip_generation:
            # Already have job file on disk - skip
            continue

        hifi_col = infos.iloc[i]['Hifi_reads']
        if hifi_col == 'NA':
            print(f'Warning: {spec_id} skipped - no PacBio reads')
            continue

        list_pacbio = hifi_col.split(',')
        custom_path = utils.get_custom_path_for_genomeark(row, assembly_id, infos, i)

        species_path = f"./{spec_id}/"
        os.makedirs(f"{species_path}job_files/", exist_ok=True)
        os.makedirs(f"{species_path}invocations_json/", exist_ok=True)
        os.makedirs(f"{species_path}planemo_log/", exist_ok=True)

        yml_file = f"{species_path}job_files/wf1_{spec_id}{suffix_run}.yml"
        res_file = f"{species_path}invocations_json/wf1_{spec_id}{suffix_run}.json"
        log_file = f"{species_path}planemo_log/{spec_id}{suffix_run}_wf1.log"

        str_elements = ""
        for file in list_pacbio:
            name = re.sub(r"\.f(ast)?q(sanger)?\.gz", "", file)
            str_elements += f"\n  - class: File\n    identifier: {name}\n    path: gxfiles://genomeark/species/{spec_name}/{assembly_id}{custom_path}/genomic_data/pacbio_hifi/{file}\n    filetype: fastqsanger.gz"

        with open(f"{path_script}/templates/wf1_run.sample.yaml", 'r') as sample_file:
            filedata = sample_file.read()

        filedata = filedata.replace('["Pacbio"]', str_elements)
        filedata = filedata.replace('["species_name"]', spec_name)
        filedata = filedata.replace('["assembly_name"]', spec_id)

        with open(yml_file, 'w') as yaml_wf1:
            yaml_wf1.write(filedata)

        cmd_line = f"planemo run {workflow_path} {yml_file} --engine external_galaxy --simultaneous_uploads --check_uploads_ok --galaxy_url {galaxy_instance} --galaxy_user_key $MAINKEY --history_name {spec_id}{suffix_run} --no_wait --test_output_json {res_file} > {log_file} 2>&1 &"
        commands.append(cmd_line)

        # Update metadata directly
        if spec_id in list_metadata:
            list_metadata[spec_id]["job_files"][workflow_key] = yml_file
            list_metadata[spec_id]["invocation_jsons"][workflow_key] = res_file
            list_metadata[spec_id]["planemo_logs"][workflow_key] = log_file
            list_metadata[spec_id]["invocations"][workflow_key] = "NA"

        print(f"Prepared: {spec_id}")

    return commands


def prepare_workflow_4(infos, gi, workflow_path, galaxy_instance, suffix_run, path_script, list_metadata=None):
    """Prepare Workflow 4 (Assembly with HiFi and Hi-C phasing)"""
    suffix_run = utils.normalize_suffix(suffix_run)
    commands = []
    workflow_key = "Workflow_4"

    for i, row in infos.iterrows():
        spec_name = str(row['Species']).strip()
        assembly_id = str(row['Assembly']).strip()
        spec_id = utils.get_working_assembly(row, infos, i)

        # Check metadata first - skip if already prepared or complete
        skip_generation, metadata_invocation, metadata_datasets = check_metadata_for_workflow(
            spec_id, workflow_key, list_metadata, gi
        )

        if skip_generation:
            # Already have job file on disk - skip
            continue

        species_path = f"./{spec_id}/"
        os.makedirs(f"{species_path}job_files/", exist_ok=True)
        os.makedirs(f"{species_path}invocations_json/", exist_ok=True)
        os.makedirs(f"{species_path}planemo_log/", exist_ok=True)
        os.makedirs(f"{species_path}reports/", exist_ok=True)

        # Get WF1 invocation number - try 3 sources in order
        invocation_number = None
        history_invocation_cache = None

        # Step 1: Try to get from JSON file
        json_wf1 = infos.iloc[i].get('Results_wf1', 'NA')
        if isinstance(json_wf1, str) and json_wf1 != 'NA' and os.path.exists(json_wf1):
            try:
                with open(json_wf1) as wf1json:
                    reswf1 = json.load(wf1json)
                    invocation_number = reswf1["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                    print(f"  {spec_id}: WF1 invocation from JSON file: {invocation_number}")
            except (json.JSONDecodeError, KeyError, IOError) as e:
                print(f"  {spec_id}: Could not read WF1 JSON: {e}")

        # Step 2: Try to get from tracking table metadata
        if not invocation_number:
            inv_col = row.get('Invocation_wf1', 'NA')
            if is_valid_invocation(inv_col):
                invocation_number = str(inv_col)
                print(f"  {spec_id}: WF1 invocation from tracking table: {invocation_number}")

        # Step 3: Try to get from Galaxy history
        if not invocation_number:
            history_id = row.get('History_id', 'NA')
            if is_valid_invocation(history_id):
                # Check if history still exists before searching
                try:
                    gi.histories.show_history(history_id)
                    print(f"  {spec_id}: Searching history {history_id} for WF1 invocation...")
                    history_invocation_cache = galaxy_client.build_invocation_cache(gi, history_id)
                    invocation_number = galaxy_client.fetch_invocation_from_history(
                        gi, history_id, "VGP1", cache=history_invocation_cache
                    )
                    if invocation_number:
                        print(f"  {spec_id}: Found WF1 invocation in history: {invocation_number}")
                except Exception as e:
                    # History has been deleted or is inaccessible
                    print(f"ERROR: {spec_id} - History {history_id} not found (likely deleted). Removing from metadata.")
                    if spec_id in list_metadata:
                        list_metadata[spec_id]['history_id'] = 'NA'

        # If still not found, skip
        if not invocation_number:
            print(f"Skipped {spec_id}: No WF1 invocation found (tried JSON, metadata, and history)")
            continue

        yml_file = f"{species_path}job_files/wf4_{spec_id}{suffix_run}.yml"
        res_file = f"{species_path}invocations_json/wf4_{spec_id}{suffix_run}.json"
        log_file = f"{species_path}planemo_log/{spec_id}{suffix_run}_wf4.log"

        if os.path.exists(yml_file):
            print(f"Skipped {spec_id}: Files already generated")
            continue

        wf1_inv = gi.invocations.show_invocation(invocation_number)
        invocation_state = gi.invocations.get_invocation_summary(invocation_number)['populated_state']

        if invocation_state != 'ok':
            print(f"Skipped {spec_id}: Invocation incomplete (status: {invocation_state})")
            continue

        dic_data_ids = galaxy_client.get_datasets_ids(wf1_inv)

        # Download report
        report_file = f"{species_path}reports/report_wf1_{spec_id}{suffix_run}_{invocation_number}.pdf"
        try:
            gi.invocations.get_invocation_report_pdf(invocation_number, file_path=report_file)
        except:
            report_file = "NA"

        # Prepare HiC data
        hic_f_col = infos.iloc[i]['HiC_forward_reads']
        hic_r_col = infos.iloc[i]['HiC_reverse_reads']
        hic_type = infos.iloc[i]['HiC_Type']

        if pandas.isna(hic_f_col) or pandas.isna(hic_r_col):
            print(f'Warning: {spec_id} skipped - missing Hi-C reads')
            continue

        hic_f = hic_f_col.split(',')
        hic_r = hic_r_col.split(',')
        custom_path = utils.get_custom_path_for_genomeark(row, assembly_id, infos, i)

        str_hic = ""
        for idx in range(len(hic_f)):
            namef = re.sub(r"\.f(ast)?q(sanger)?\.gz", "", hic_f[idx])
            str_hic += f"\n  - class: Collection\n    type: paired\n    identifier: {namef}\n    elements:\n    - identifier: forward\n      class: File\n      path: gxfiles://genomeark/species/{spec_name}/{assembly_id}{custom_path}/genomic_data/{hic_type}/{hic_f[idx]}\n      filetype: fastqsanger.gz\n    - identifier: reverse\n      class: File\n      path: gxfiles://genomeark/species/{spec_name}/{assembly_id}{custom_path}/genomic_data/{hic_type}/{hic_r[idx]}\n      filetype: fastqsanger.gz"

        history_id = wf1_inv['history_id']

        # Load template and fill
        with open(f"{path_script}/templates/wf4_run.sample.yaml", 'r') as sample_file:
            filedata = sample_file.read()

        pattern = r'\["(.*)"\]'
        to_fill = re.findall(pattern, filedata)
        dic_data_ids['hic'] = str_hic

        for field in to_fill:
            filedata = filedata.replace(f'["{field}"]', dic_data_ids.get(field, ''))

        with open(yml_file, 'w') as yaml_wf4:
            yaml_wf4.write(filedata)

        cmd_line = f"planemo run {workflow_path} {yml_file} --engine external_galaxy --simultaneous_uploads --check_uploads_ok --galaxy_url {galaxy_instance} --galaxy_user_key $MAINKEY --history_id {history_id} --no_wait --test_output_json {res_file} > {log_file} 2>&1 &"
        commands.append(cmd_line)

        # Update metadata directly
        if spec_id in list_metadata:
            list_metadata[spec_id]["job_files"][workflow_key] = yml_file
            list_metadata[spec_id]["invocation_jsons"][workflow_key] = res_file
            list_metadata[spec_id]["planemo_logs"][workflow_key] = log_file
            list_metadata[spec_id]["invocations"][workflow_key] = "NA"
            list_metadata[spec_id]["history_id"] = history_id
            if report_file != "NA":
                list_metadata[spec_id]["reports"]["Workflow_1"] = report_file

        print(f"Prepared: {spec_id}")

    return commands


def prepare_workflow_8(infos, gi, workflow_path, galaxy_instance, suffix_run, path_script, haplotype, list_metadata=None):
    """Prepare Workflow 8 (Haplotype-specific assembly)"""
    suffix_run = utils.normalize_suffix(suffix_run)
    commands = []
    workflow_key = f"Workflow_8_{haplotype}"

    for i, row in infos.iterrows():
        spec_name = str(row['Species']).strip()
        assembly_id = str(row['Assembly']).strip()
        spec_id = utils.get_working_assembly(row, infos, i)

        # Check metadata first - skip if already prepared or complete
        skip_generation, metadata_invocation, metadata_datasets = check_metadata_for_workflow(
            spec_id, workflow_key, list_metadata, gi
        )

        if skip_generation:
            # Already have job file on disk - skip
            continue

        species_path = f"./{spec_id}/"
        os.makedirs(f"{species_path}job_files/", exist_ok=True)
        os.makedirs(f"{species_path}invocations_json/", exist_ok=True)
        os.makedirs(f"{species_path}planemo_log/", exist_ok=True)

        # Get WF4 invocation number - try 3 sources in order
        invocation_number = None
        history_invocation_cache = None

        # Step 1: Try to get from JSON file
        json_wf4 = infos.iloc[i].get('Results_wf4', 'NA')
        if isinstance(json_wf4, str) and json_wf4 != 'NA' and os.path.exists(json_wf4):
            try:
                with open(json_wf4) as wf4json:
                    reswf4 = json.load(wf4json)
                    invocation_number = reswf4["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                    print(f"  {spec_id}: WF4 invocation from JSON file: {invocation_number}")
            except (json.JSONDecodeError, KeyError, IOError) as e:
                print(f"  {spec_id}: Could not read WF4 JSON: {e}")

        # Step 2: Try to get from tracking table metadata
        if not invocation_number:
            inv_col = row.get('Invocation_wf4', 'NA')
            if is_valid_invocation(inv_col):
                invocation_number = str(inv_col)
                print(f"  {spec_id}: WF4 invocation from tracking table: {invocation_number}")

        # Step 3: Try to get from Galaxy history
        if not invocation_number:
            history_id = row.get('History_id', 'NA')
            if is_valid_invocation(history_id):
                try:
                    gi.histories.show_history(history_id)
                    print(f"  {spec_id}: Searching history for WF4 invocation...")
                    history_invocation_cache = galaxy_client.build_invocation_cache(gi, history_id)
                    invocation_number = galaxy_client.fetch_invocation_from_history(
                        gi, history_id, "VGP4", cache=history_invocation_cache
                    )
                    if invocation_number:
                        print(f"  {spec_id}: Found WF4 invocation in history: {invocation_number}")
                except Exception as e:
                    print(f"ERROR: {spec_id} - History {history_id} not found (likely deleted). Removing from metadata.")
                    if spec_id in list_metadata:
                        list_metadata[spec_id]['history_id'] = 'NA'

        # If still not found, skip
        if not invocation_number:
            print(f"Skipped {spec_id}: No WF4 invocation found (tried JSON, metadata, and history)")
            continue

        yml_file = f"{species_path}job_files/wf8_{spec_id}{suffix_run}_{haplotype}.yml"
        res_file = f"{species_path}invocations_json/wf8_{spec_id}{suffix_run}_{haplotype}.json"
        log_file = f"{species_path}planemo_log/{spec_id}{suffix_run}_wf8_{haplotype}.log"

        if os.path.exists(yml_file):
            print(f"Skipped {spec_id} ({haplotype}): Files already generated")
            continue

        wf4_inv = gi.invocations.show_invocation(invocation_number)
        invocation_state = gi.invocations.get_invocation_summary(invocation_number)['populated_state']

        if invocation_state != 'ok':
            print(f"Skipped {spec_id}: WF4 invocation incomplete (status: {invocation_state})")
            continue

        dic_data_ids = galaxy_client.get_datasets_ids(wf4_inv)

        # Prepare HiC data
        hic_f_col = infos.iloc[i]['HiC_forward_reads']
        hic_r_col = infos.iloc[i]['HiC_reverse_reads']
        hic_type = infos.iloc[i]['HiC_Type']

        if pandas.isna(hic_f_col) or pandas.isna(hic_r_col):
            print(f'Warning: {spec_id} skipped - missing Hi-C reads')
            continue

        hic_f = hic_f_col.split(',')
        hic_r = hic_r_col.split(',')
        custom_path = utils.get_custom_path_for_genomeark(row, assembly_id, infos, i)

        str_hic = ""
        for idx in range(len(hic_f)):
            namef = re.sub(r"\.f(ast)?q(sanger)?\.gz", "", hic_f[idx])
            str_hic += f"\n  - class: Collection\n    type: paired\n    identifier: {namef}\n    elements:\n    - identifier: forward\n      class: File\n      path: gxfiles://genomeark/species/{spec_name}/{assembly_id}{custom_path}/genomic_data/{hic_type}/{hic_f[idx]}\n      filetype: fastqsanger.gz\n    - identifier: reverse\n      class: File\n      path: gxfiles://genomeark/species/{spec_name}/{assembly_id}{custom_path}/genomic_data/{hic_type}/{hic_r[idx]}\n      filetype: fastqsanger.gz"

        history_id = wf4_inv['history_id']

        # Load template and fill
        with open(f"{path_script}/templates/wf8_run_sample.yaml", 'r') as sample_file:
            filedata = sample_file.read()

        pattern = r'\["(.*)"\]'
        to_fill = re.findall(pattern, filedata)
        dic_data_ids['hic'] = str_hic
        dic_data_ids['haplotype'] = haplotype

        for field in to_fill:
            filedata = filedata.replace(f'["{field}"]', dic_data_ids.get(field, ''))

        with open(yml_file, 'w') as yaml_wf8:
            yaml_wf8.write(filedata)

        cmd_line = f"planemo run {workflow_path} {yml_file} --engine external_galaxy --simultaneous_uploads --check_uploads_ok --galaxy_url {galaxy_instance} --galaxy_user_key $MAINKEY --history_id {history_id} --no_wait --test_output_json {res_file} > {log_file} 2>&1 &"
        commands.append(cmd_line)

        # Update metadata directly
        if spec_id in list_metadata:
            list_metadata[spec_id]["job_files"][workflow_key] = yml_file
            list_metadata[spec_id]["invocation_jsons"][workflow_key] = res_file
            list_metadata[spec_id]["planemo_logs"][workflow_key] = log_file
            list_metadata[spec_id]["invocations"][workflow_key] = "NA"
            list_metadata[spec_id]["history_id"] = history_id

        print(f"Prepared: {spec_id} ({haplotype})")

    return commands


def prepare_workflow_9(infos, gi, workflow_path, galaxy_instance, suffix_run, path_script, haplotype, template_file, list_metadata=None):
    """Prepare Workflow 9 (Decontamination)"""
    suffix_run = utils.normalize_suffix(suffix_run)
    commands = []

    inv_col = f'Invocation_wf8_{haplotype}'
    res_col = f'Results_wf8_{haplotype}'

    for i, row in infos.iterrows():
        spec_name = str(row['Species']).strip()
        assembly_id = str(row['Assembly']).strip()
        spec_id = utils.get_working_assembly(row, infos, i)

        # Check metadata first - skip if already prepared or complete
        workflow_key = f"Workflow_9_{haplotype}"
        skip_generation, metadata_invocation, metadata_datasets = check_metadata_for_workflow(
            spec_id, workflow_key, list_metadata, gi
        )

        if skip_generation:
            continue

        species_path = f"./{spec_id}/"
        os.makedirs(f"{species_path}job_files/", exist_ok=True)
        os.makedirs(f"{species_path}invocations_json/", exist_ok=True)
        os.makedirs(f"{species_path}planemo_log/", exist_ok=True)

        # Get WF8 invocation number - try 3 sources in order
        invocation_number = None
        history_invocation_cache = None

        # Step 1: Try to get from JSON file
        json_wf8 = infos.iloc[i].get(res_col, 'NA')
        if isinstance(json_wf8, str) and json_wf8 != 'NA' and os.path.exists(json_wf8):
            try:
                with open(json_wf8) as wf8json:
                    reswf8 = json.load(wf8json)
                    invocation_number = reswf8["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                    print(f"  {spec_id} ({haplotype}): WF8 invocation from JSON file: {invocation_number}")
            except (json.JSONDecodeError, KeyError, IOError) as e:
                print(f"  {spec_id} ({haplotype}): Could not read WF8 JSON: {e}")

        # Step 2: Try to get from tracking table metadata
        if not invocation_number:
            invocation_col = row.get(inv_col, 'NA')
            if is_valid_invocation(invocation_col):
                invocation_number = str(invocation_col)
                print(f"  {spec_id} ({haplotype}): WF8 invocation from tracking table: {invocation_number}")

        # Step 3: Try to get from Galaxy history
        if not invocation_number:
            history_id = row.get('History_id', 'NA')
            if is_valid_invocation(history_id):
                try:
                    gi.histories.show_history(history_id)
                    print(f"  {spec_id} ({haplotype}): Searching history for WF8 invocation...")
                    history_invocation_cache = galaxy_client.build_invocation_cache(gi, history_id)
                    invocation_number = galaxy_client.fetch_invocation_from_history(
                        gi, history_id, "VGP8", haplotype=haplotype, cache=history_invocation_cache
                    )
                    if invocation_number:
                        print(f"  {spec_id} ({haplotype}): Found WF8 invocation in history: {invocation_number}")
                except Exception as e:
                    print(f"ERROR: {spec_id} - History {history_id} not found (likely deleted). Removing from metadata.")
                    if spec_id in list_metadata:
                        list_metadata[spec_id]['history_id'] = 'NA'

        # If still not found, skip
        if not invocation_number:
            print(f"Skipped {spec_id}: No WF8 invocation found for {haplotype} (tried JSON, metadata, and history)")
            continue

        yml_file = f"{species_path}job_files/wf9_{spec_id}{suffix_run}_{haplotype}.yml"
        res_file = f"{species_path}invocations_json/wf9_{spec_id}{suffix_run}_{haplotype}.json"
        log_file = f"{species_path}planemo_log/{spec_id}{suffix_run}_wf9_{haplotype}.log"

        wf8_inv = gi.invocations.show_invocation(invocation_number)
        invocation_state = gi.invocations.get_invocation_summary(invocation_number)['populated_state']

        if invocation_state != 'ok':
            print(f"Skipped {spec_id}: WF8 invocation incomplete (status: {invocation_state})")
            continue

        dic_data_ids = galaxy_client.get_datasets_ids(wf8_inv)
        history_id = wf8_inv['history_id']

        # Load template and fill
        with open(f"{path_script}/templates/{template_file}", 'r') as sample_file:
            filedata = sample_file.read()

        pattern = r'\["(.*)"\]'
        to_fill = re.findall(pattern, filedata)
        dic_data_ids['haplotype'] = haplotype

        # Get taxon ID if available
        if 'taxon_ID' in infos.columns and pandas.notna(infos.iloc[i]['taxon_ID']):
            dic_data_ids['taxon_ID'] = str(infos.iloc[i]['taxon_ID'])

        for field in to_fill:
            filedata = filedata.replace(f'["{field}"]', dic_data_ids.get(field, ''))

        with open(yml_file, 'w') as yaml_wf9:
            yaml_wf9.write(filedata)

        cmd_line = f"planemo run {workflow_path} {yml_file} --engine external_galaxy --simultaneous_uploads --check_uploads_ok --galaxy_url {galaxy_instance} --galaxy_user_key $MAINKEY --history_id {history_id} --no_wait --test_output_json {res_file} > {log_file} 2>&1 &"
        commands.append(cmd_line)
        print(f"Prepared: {spec_id} ({haplotype})")

        # Update metadata
        if spec_id in list_metadata:
            list_metadata[spec_id]["job_files"][workflow_key] = yml_file
            list_metadata[spec_id]["invocation_jsons"][workflow_key] = res_file
            list_metadata[spec_id]["planemo_logs"][workflow_key] = log_file
            list_metadata[spec_id]["invocations"][workflow_key] = "NA"
            list_metadata[spec_id]["history_id"] = history_id

    return commands


def prepare_workflow_0(infos, gi, workflow_path, galaxy_instance, suffix_run, path_script, list_metadata=None):
    """Prepare Workflow 0 (Mitochondrial assembly)"""
    suffix_run = utils.normalize_suffix(suffix_run)
    commands = []

    for i, row in infos.iterrows():
        spec_name = str(row['Species']).strip()
        assembly_id = str(row['Assembly']).strip()
        spec_id = utils.get_working_assembly(row, infos, i)

        # Check metadata first - skip if already prepared or complete
        workflow_key = "Workflow_0"
        skip_generation, metadata_invocation, metadata_datasets = check_metadata_for_workflow(
            spec_id, workflow_key, list_metadata, gi
        )

        if skip_generation:
            continue

        species_path = f"./{spec_id}/"
        os.makedirs(f"{species_path}job_files/", exist_ok=True)
        os.makedirs(f"{species_path}invocations_json/", exist_ok=True)
        os.makedirs(f"{species_path}planemo_log/", exist_ok=True)

        # Get WF4 invocation number - try 3 sources in order
        invocation_number = None
        history_invocation_cache = None

        # Step 1: Try to get from JSON file
        json_wf4 = infos.iloc[i].get('Results_wf4', 'NA')
        if isinstance(json_wf4, str) and json_wf4 != 'NA' and os.path.exists(json_wf4):
            try:
                with open(json_wf4) as wf4json:
                    reswf4 = json.load(wf4json)
                    invocation_number = reswf4["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                    print(f"  {spec_id}: WF4 invocation from JSON file: {invocation_number}")
            except (json.JSONDecodeError, KeyError, IOError) as e:
                print(f"  {spec_id}: Could not read WF4 JSON: {e}")

        # Step 2: Try to get from tracking table metadata
        if not invocation_number:
            inv_col = row.get('Invocation_wf4', 'NA')
            if is_valid_invocation(inv_col):
                invocation_number = str(inv_col)
                print(f"  {spec_id}: WF4 invocation from tracking table: {invocation_number}")

        # Step 3: Try to get from Galaxy history
        if not invocation_number:
            history_id = row.get('History_id', 'NA')
            if is_valid_invocation(history_id):
                try:
                    gi.histories.show_history(history_id)
                    print(f"  {spec_id}: Searching history for WF4 invocation...")
                    history_invocation_cache = galaxy_client.build_invocation_cache(gi, history_id)
                    invocation_number = galaxy_client.fetch_invocation_from_history(
                        gi, history_id, "VGP4", cache=history_invocation_cache
                    )
                    if invocation_number:
                        print(f"  {spec_id}: Found WF4 invocation in history: {invocation_number}")
                except Exception as e:
                    print(f"ERROR: {spec_id} - History {history_id} not found (likely deleted). Removing from metadata.")
                    if spec_id in list_metadata:
                        list_metadata[spec_id]['history_id'] = 'NA'

        # If still not found, skip
        if not invocation_number:
            print(f"Skipped {spec_id}: No WF4 invocation found (tried JSON, metadata, and history)")
            continue

        yml_file = f"{species_path}job_files/wf0_{spec_id}{suffix_run}.yml"
        res_file = f"{species_path}invocations_json/wf0_{spec_id}{suffix_run}.json"
        log_file = f"{species_path}planemo_log/{spec_id}{suffix_run}_wf0.log"

        wf4_inv = gi.invocations.show_invocation(invocation_number)
        history_id = wf4_inv['history_id']

        # Note: WF0 can be launched even if WF4 is not complete (fire and forget)
        invocation_state = gi.invocations.get_invocation_summary(invocation_number)['populated_state']
        if invocation_state != 'ok':
            print(f"Warning: {spec_id} - WF4 not complete (status: {invocation_state}), but preparing WF0 anyway")

        dic_data_ids = galaxy_client.get_datasets_ids(wf4_inv)

        # Load template and fill
        with open(f"{path_script}/templates/wf0_run.sample.yaml", 'r') as sample_file:
            filedata = sample_file.read()

        pattern = r'\["(.*)"\]'
        to_fill = re.findall(pattern, filedata)

        for field in to_fill:
            filedata = filedata.replace(f'["{field}"]', dic_data_ids.get(field, ''))

        with open(yml_file, 'w') as yaml_wf0:
            yaml_wf0.write(filedata)

        cmd_line = f"planemo run {workflow_path} {yml_file} --engine external_galaxy --simultaneous_uploads --check_uploads_ok --galaxy_url {galaxy_instance} --galaxy_user_key $MAINKEY --history_id {history_id} --no_wait --test_output_json {res_file} > {log_file} 2>&1 &"
        commands.append(cmd_line)
        print(f"Prepared: {spec_id}")

        # Update metadata
        if spec_id in list_metadata:
            list_metadata[spec_id]["job_files"][workflow_key] = yml_file
            list_metadata[spec_id]["invocation_jsons"][workflow_key] = res_file
            list_metadata[spec_id]["planemo_logs"][workflow_key] = log_file
            list_metadata[spec_id]["invocations"][workflow_key] = "NA"
            list_metadata[spec_id]["history_id"] = history_id

    return commands


def prepare_precuration(infos, gi, workflow_path, galaxy_instance, suffix_run, path_script, list_metadata=None):
    """Prepare Pre-curation workflow (PretextMap generation)

    Requires completed WF4 and both WF9 haplotypes.
    Generates PretextMap for manual curation.
    """
    suffix_run = utils.normalize_suffix(suffix_run)
    commands = []

    for i, row in infos.iterrows():
        spec_name = str(row['Species']).strip()
        assembly_id = str(row['Assembly']).strip()
        spec_id = utils.get_working_assembly(row, infos, i)

        # Check metadata first - skip if already prepared or complete
        workflow_key = "Workflow_PreCuration"
        skip_generation, metadata_invocation, metadata_datasets = check_metadata_for_workflow(
            spec_id, workflow_key, list_metadata, gi
        )

        if skip_generation:
            continue

        species_path = f"./{spec_id}/"
        os.makedirs(f"{species_path}job_files/", exist_ok=True)
        os.makedirs(f"{species_path}invocations_json/", exist_ok=True)
        os.makedirs(f"{species_path}planemo_log/", exist_ok=True)

        # Get required invocations using 3-step pattern
        history_invocation_cache = None

        # === Fetch WF4 invocation ===
        inv_wf4 = None

        # Step 1: Try JSON file
        json_wf4 = infos.iloc[i].get('Results_wf4', 'NA')
        if isinstance(json_wf4, str) and json_wf4 != 'NA' and os.path.exists(json_wf4):
            try:
                with open(json_wf4) as wf4json:
                    reswf4 = json.load(wf4json)
                    inv_wf4 = reswf4["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                    print(f"  {spec_id}: WF4 invocation from JSON file: {inv_wf4}")
            except (json.JSONDecodeError, KeyError, IOError) as e:
                print(f"  {spec_id}: Could not read WF4 JSON: {e}")

        # Step 2: Try tracking table
        if not inv_wf4:
            inv_col = row.get('Invocation_wf4', 'NA')
            if is_valid_invocation(inv_col):
                inv_wf4 = str(inv_col)
                print(f"  {spec_id}: WF4 invocation from tracking table: {inv_wf4}")

        # Step 3: Try history search
        history_deleted = False
        if not inv_wf4:
            history_id = row.get('History_id', 'NA')
            if is_valid_invocation(history_id):
                try:
                    gi.histories.show_history(history_id)
                    print(f"  {spec_id}: Searching history for WF4 invocation...")
                    history_invocation_cache = galaxy_client.build_invocation_cache(gi, history_id)
                    inv_wf4 = galaxy_client.fetch_invocation_from_history(
                        gi, history_id, "VGP4", cache=history_invocation_cache
                    )
                    if inv_wf4:
                        print(f"  {spec_id}: Found WF4 invocation in history: {inv_wf4}")
                except Exception as e:
                    print(f"ERROR: {spec_id} - History {history_id} not found (likely deleted). Removing from metadata.")
                    history_deleted = True
                    if spec_id in list_metadata:
                        list_metadata[spec_id]['history_id'] = 'NA'

        # === Fetch WF9 hap1 invocation ===
        inv_wf9_hap1 = None

        # Step 1: Try JSON file
        json_wf9_hap1 = infos.iloc[i].get('Results_wf9_hap1', 'NA')
        if isinstance(json_wf9_hap1, str) and json_wf9_hap1 != 'NA' and os.path.exists(json_wf9_hap1):
            try:
                with open(json_wf9_hap1) as wf9json:
                    reswf9 = json.load(wf9json)
                    inv_wf9_hap1 = reswf9["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                    print(f"  {spec_id}: WF9 hap1 invocation from JSON file: {inv_wf9_hap1}")
            except (json.JSONDecodeError, KeyError, IOError) as e:
                print(f"  {spec_id}: Could not read WF9 hap1 JSON: {e}")

        # Step 2: Try tracking table
        if not inv_wf9_hap1:
            inv_col = row.get('Invocation_wf9_hap1', 'NA')
            if is_valid_invocation(inv_col):
                inv_wf9_hap1 = str(inv_col)
                print(f"  {spec_id}: WF9 hap1 invocation from tracking table: {inv_wf9_hap1}")

        # Step 3: Try history search
        if not inv_wf9_hap1 and not history_deleted:
            history_id = row.get('History_id', 'NA')
            if is_valid_invocation(history_id):
                try:
                    if not history_invocation_cache:
                        gi.histories.show_history(history_id)
                        history_invocation_cache = galaxy_client.build_invocation_cache(gi, history_id)
                    print(f"  {spec_id}: Searching history for WF9 hap1 invocation...")
                    inv_wf9_hap1 = galaxy_client.fetch_invocation_from_history(
                        gi, history_id, "VGP9", haplotype='hap1', cache=history_invocation_cache
                    )
                    if inv_wf9_hap1:
                        print(f"  {spec_id}: Found WF9 hap1 invocation in history: {inv_wf9_hap1}")
                except Exception as e:
                    if not history_deleted:
                        print(f"ERROR: {spec_id} - History {history_id} not found (likely deleted). Removing from metadata.")
                        history_deleted = True
                        if spec_id in list_metadata:
                            list_metadata[spec_id]['history_id'] = 'NA'

        # === Fetch WF9 hap2 invocation ===
        inv_wf9_hap2 = None

        # Step 1: Try JSON file
        json_wf9_hap2 = infos.iloc[i].get('Results_wf9_hap2', 'NA')
        if isinstance(json_wf9_hap2, str) and json_wf9_hap2 != 'NA' and os.path.exists(json_wf9_hap2):
            try:
                with open(json_wf9_hap2) as wf9json:
                    reswf9 = json.load(wf9json)
                    inv_wf9_hap2 = reswf9["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                    print(f"  {spec_id}: WF9 hap2 invocation from JSON file: {inv_wf9_hap2}")
            except (json.JSONDecodeError, KeyError, IOError) as e:
                print(f"  {spec_id}: Could not read WF9 hap2 JSON: {e}")

        # Step 2: Try tracking table
        if not inv_wf9_hap2:
            inv_col = row.get('Invocation_wf9_hap2', 'NA')
            if is_valid_invocation(inv_col):
                inv_wf9_hap2 = str(inv_col)
                print(f"  {spec_id}: WF9 hap2 invocation from tracking table: {inv_wf9_hap2}")

        # Step 3: Try history search
        if not inv_wf9_hap2 and not history_deleted:
            history_id = row.get('History_id', 'NA')
            if is_valid_invocation(history_id):
                try:
                    if not history_invocation_cache:
                        gi.histories.show_history(history_id)
                        history_invocation_cache = galaxy_client.build_invocation_cache(gi, history_id)
                    print(f"  {spec_id}: Searching history for WF9 hap2 invocation...")
                    inv_wf9_hap2 = galaxy_client.fetch_invocation_from_history(
                        gi, history_id, "VGP9", haplotype='hap2', cache=history_invocation_cache
                    )
                    if inv_wf9_hap2:
                        print(f"  {spec_id}: Found WF9 hap2 invocation in history: {inv_wf9_hap2}")
                except Exception as e:
                    if not history_deleted:
                        print(f"ERROR: {spec_id} - History {history_id} not found (likely deleted). Removing from metadata.")
                        history_deleted = True
                        if spec_id in list_metadata:
                            list_metadata[spec_id]['history_id'] = 'NA'

        # Check if all required invocations found
        if not inv_wf4 or not inv_wf9_hap1 or not inv_wf9_hap2:
            missing = []
            if not inv_wf4:
                missing.append("WF4")
            if not inv_wf9_hap1:
                missing.append("WF9 hap1")
            if not inv_wf9_hap2:
                missing.append("WF9 hap2")
            print(f"Skipped {spec_id}: Missing required invocations: {', '.join(missing)} (tried JSON, metadata, and history)")
            continue

        yml_file = f"{species_path}job_files/precuration_{spec_id}{suffix_run}.yml"
        res_file = f"{species_path}invocations_json/precuration_{spec_id}{suffix_run}.json"
        log_file = f"{species_path}planemo_log/{spec_id}{suffix_run}_precuration.log"

        # Get invocation objects
        try:
            wf4_inv = gi.invocations.show_invocation(str(inv_wf4))
            wf9_hap1_inv = gi.invocations.show_invocation(str(inv_wf9_hap1))
            wf9_hap2_inv = gi.invocations.show_invocation(str(inv_wf9_hap2))
        except Exception as e:
            print(f"Error: {spec_id} - Could not fetch invocations: {e}")
            continue

        # Check that WF4 and both WF9s are complete
        wf4_state = gi.invocations.get_invocation_summary(str(inv_wf4))['populated_state']
        wf9_hap1_state = gi.invocations.get_invocation_summary(str(inv_wf9_hap1))['populated_state']
        wf9_hap2_state = gi.invocations.get_invocation_summary(str(inv_wf9_hap2))['populated_state']

        if wf4_state != 'ok':
            print(f"Skipped {spec_id}: WF4 not complete (status: {wf4_state})")
            continue

        if wf9_hap1_state != 'ok':
            print(f"Skipped {spec_id}: WF9 hap1 not complete (status: {wf9_hap1_state})")
            continue

        if wf9_hap2_state != 'ok':
            print(f"Skipped {spec_id}: WF9 hap2 not complete (status: {wf9_hap2_state})")
            continue

        # Get dataset IDs from workflows
        dic_data_ids_wf4 = galaxy_client.get_datasets_ids(wf4_inv)
        dic_data_ids_wf9_hap1 = galaxy_client.get_datasets_ids(wf9_hap1_inv)
        dic_data_ids_wf9_hap2 = galaxy_client.get_datasets_ids(wf9_hap2_inv)

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
        dic_data_ids['trimhic'] = 'false'  # Reads already trimmed by WF4

        # Load template and fill
        with open(f"{path_script}/templates/precuration_run.sample.yaml", 'r') as sample_file:
            filedata = sample_file.read()

        pattern = r'\["(.*)"\]'
        to_fill = re.findall(pattern, filedata)

        for field in to_fill:
            if field in dic_data_ids:
                filedata = filedata.replace(f'["{field}"]', dic_data_ids[field])
            else:
                print(f"Warning: Field '{field}' not found in data for {spec_id}")

        with open(yml_file, 'w') as yaml_precuration:
            yaml_precuration.write(filedata)

        # Use WF4 history for pre-curation
        history_id = wf4_inv['history_id']

        cmd_line = f"planemo run {workflow_path} {yml_file} --engine external_galaxy --simultaneous_uploads --check_uploads_ok --galaxy_url {galaxy_instance} --galaxy_user_key $MAINKEY --history_id {history_id} --no_wait --test_output_json {res_file} > {log_file} 2>&1 &"
        commands.append(cmd_line)
        print(f"Prepared: {spec_id}")

        # Update metadata
        if spec_id in list_metadata:
            list_metadata[spec_id]["job_files"][workflow_key] = yml_file
            list_metadata[spec_id]["invocation_jsons"][workflow_key] = res_file
            list_metadata[spec_id]["planemo_logs"][workflow_key] = log_file
            list_metadata[spec_id]["invocations"][workflow_key] = "NA"
            list_metadata[spec_id]["history_id"] = history_id

    return commands


def update_metadata_from_table(track_table, metadata_dir, suffix_run, gi):
    """Update metadata from tracking table (table values take precedence)"""
    suffix_run = utils.normalize_suffix(suffix_run)
    print(f"{'='*60}")
    print("Updating metadata from tracking table")
    print(f"{'='*60}\n")

    # Load tracking table
    infos = pandas.read_csv(track_table, header=0, sep="\t")

    # Load existing metadata
    metadata_file = f"{metadata_dir}metadata_run{suffix_run}.json"
    if not os.path.isfile(metadata_file):
        print(f"No existing metadata found at: {metadata_file}")
        print("Creating new metadata from tracking table...\n")
        list_metadata = {}
    else:
        print(f"Loading metadata: {metadata_file}\n")
        with open(metadata_file, "r") as json_file:
            list_metadata = json.load(json_file)

    changes_count = 0
    new_species_count = 0
    invocations_updated = []

    for i, row in infos.iterrows():
        spec_name = str(row['Species']).strip()
        assembly_id = str(row['Assembly']).strip()
        working_assembly = utils.get_working_assembly(row, infos, i)

        # Initialize if new species
        if working_assembly not in list_metadata:
            print(f"✓ {working_assembly}: New species, creating metadata entry")
            new_species_count += 1
            list_metadata[working_assembly] = {
                'Assembly': assembly_id,
                'History_name': working_assembly + suffix_run,
                'Name': spec_name,
                'Custom_Path': str(infos.iloc[i]['Custom_Path']).strip() if 'Custom_Path' in infos.columns and pandas.notna(infos.iloc[i]['Custom_Path']) else '',
                'Path': f"./{working_assembly}/",
                'job_files': {},
                'invocation_jsons': {},
                'planemo_logs': {},
                'reports': {},
                'invocations': {},
                'dataset_ids': {},
                'history_id': 'NA',
                'taxon_id': 'NA',
                'failed_invocations': {}
            }

        # Update genomic data from table
        if 'Hifi_reads' in infos.columns:
            hifi_col = infos.iloc[i]['Hifi_reads']
            if hifi_col != 'NA' and pandas.notna(hifi_col):
                new_value = hifi_col.split(',')
                old_value = list_metadata[working_assembly].get('Hifi_reads', [])
                if new_value != old_value:
                    list_metadata[working_assembly]['Hifi_reads'] = new_value
                    print(f"  {working_assembly}: Updated Hifi_reads")
                    changes_count += 1
            elif 'Hifi_reads' not in list_metadata[working_assembly]:
                list_metadata[working_assembly]['Hifi_reads'] = []

        if 'HiC_forward_reads' in infos.columns and 'HiC_reverse_reads' in infos.columns:
            hic_f_col = infos.iloc[i]['HiC_forward_reads']
            hic_r_col = infos.iloc[i]['HiC_reverse_reads']
            if pandas.notna(hic_f_col) and pandas.notna(hic_r_col):
                new_f = hic_f_col.split(',')
                new_r = hic_r_col.split(',')
                old_f = list_metadata[working_assembly].get('HiC_forward_reads', [])
                old_r = list_metadata[working_assembly].get('HiC_reverse_reads', [])
                if new_f != old_f or new_r != old_r:
                    list_metadata[working_assembly]['HiC_forward_reads'] = new_f
                    list_metadata[working_assembly]['HiC_reverse_reads'] = new_r
                    print(f"  {working_assembly}: Updated HiC reads")
                    changes_count += 1

        if 'HiC_Type' in infos.columns:
            hic_type = infos.iloc[i]['HiC_Type']
            if pandas.notna(hic_type) and hic_type != 'NA':
                old_type = list_metadata[working_assembly].get('HiC_Type')
                if hic_type != old_type:
                    list_metadata[working_assembly]['HiC_Type'] = hic_type
                    print(f"  {working_assembly}: Updated HiC_Type")
                    changes_count += 1

        # Update invocations from table
        invocation_cols = {
            'Invocation_wf1': 'Workflow_1',
            'Invocation_wf0': 'Workflow_0',
            'Invocation_wf4': 'Workflow_4',
            'Invocation_wf8_hap1': 'Workflow_8_hap1',
            'Invocation_wf8_hap2': 'Workflow_8_hap2',
            'Invocation_wf9_hap1': 'Workflow_9_hap1',
            'Invocation_wf9_hap2': 'Workflow_9_hap2'
        }

        for col_name, wf_key in invocation_cols.items():
            if col_name in infos.columns:
                inv_value = infos.iloc[i][col_name]
                if pandas.notna(inv_value) and inv_value != 'NA':
                    inv_value = str(inv_value).strip()
                    old_inv = list_metadata[working_assembly]['invocations'].get(wf_key, 'NA')
                    if inv_value != old_inv:
                        list_metadata[working_assembly]['invocations'][wf_key] = inv_value
                        print(f"  {working_assembly}: Updated {wf_key} invocation → {inv_value}")
                        changes_count += 1
                        invocations_updated.append((working_assembly, wf_key, inv_value))

    # Fetch invocation details for updated invocations
    if invocations_updated and gi:
        print(f"\nFetching invocation details from Galaxy...")
        for working_assembly, wf_key, inv_id in invocations_updated:
            try:
                print(f"  {working_assembly}/{wf_key}: Fetching invocation {inv_id}...")
                inv_details = gi.invocations.show_invocation(inv_id)

                # Get dataset IDs
                dataset_ids = galaxy_client.get_datasets_ids(inv_details)
                if wf_key not in list_metadata[working_assembly]['dataset_ids']:
                    list_metadata[working_assembly]['dataset_ids'][wf_key] = {}
                list_metadata[working_assembly]['dataset_ids'][wf_key] = dataset_ids

                # Get history ID
                if 'history_id' in inv_details:
                    list_metadata[working_assembly]['history_id'] = inv_details['history_id']

                print(f"    ✓ Dataset IDs extracted: {len(dataset_ids)} datasets")
            except Exception as e:
                print(f"    ⚠ Warning: Could not fetch invocation {inv_id}: {e}")

    # Save metadata
    with open(metadata_file, "w") as json_file:
        json.dump(list_metadata, json_file, indent=4)

    print(f"\n{'='*60}")
    print(f"✓ Metadata updated:")
    print(f"  - {new_species_count} new species added")
    print(f"  - {changes_count} fields updated")
    print(f"  - {len(invocations_updated)} invocations updated with dataset IDs")
    print(f"  Saved: {metadata_file}")
    print(f"{'='*60}\n")


def update_table_from_metadata(track_table, metadata_dir, suffix_run):
    """Update tracking table from metadata (metadata values take precedence)"""
    suffix_run = utils.normalize_suffix(suffix_run)
    print(f"{'='*60}")
    print("Updating tracking table from metadata")
    print(f"{'='*60}\n")

    # Load metadata
    metadata_file = f"{metadata_dir}metadata_run{suffix_run}.json"
    if not os.path.isfile(metadata_file):
        raise SystemExit(f"Error: Metadata file not found: {metadata_file}")

    print(f"Loading metadata: {metadata_file}\n")
    with open(metadata_file, "r") as json_file:
        list_metadata = json.load(json_file)

    # Load tracking table
    infos = pandas.read_csv(track_table, header=0, sep="\t")

    changes_count = 0
    columns_added = []

    # Ensure required columns exist
    required_cols = ['Hifi_reads', 'HiC_forward_reads', 'HiC_reverse_reads', 'HiC_Type']
    for col in required_cols:
        if col not in infos.columns:
            infos[col] = 'NA'
            columns_added.append(col)

    for i, row in infos.iterrows():
        spec_name = str(row['Species']).strip()
        assembly_id = str(row['Assembly']).strip()
        working_assembly = utils.get_working_assembly(row, infos, i)

        if working_assembly not in list_metadata:
            print(f"  {working_assembly}: Not in metadata, keeping table values")
            continue

        print(f"  {working_assembly}: Updating from metadata")

        # Update genomic data
        if 'Hifi_reads' in list_metadata[working_assembly] and list_metadata[working_assembly]['Hifi_reads']:
            hifi_str = ','.join(list_metadata[working_assembly]['Hifi_reads'])
            if str(infos.at[i, 'Hifi_reads']) != hifi_str:
                infos.at[i, 'Hifi_reads'] = hifi_str
                changes_count += 1

        if 'HiC_forward_reads' in list_metadata[working_assembly] and list_metadata[working_assembly]['HiC_forward_reads']:
            hic_f_str = ','.join(list_metadata[working_assembly]['HiC_forward_reads'])
            if str(infos.at[i, 'HiC_forward_reads']) != hic_f_str:
                infos.at[i, 'HiC_forward_reads'] = hic_f_str
                changes_count += 1

        if 'HiC_reverse_reads' in list_metadata[working_assembly] and list_metadata[working_assembly]['HiC_reverse_reads']:
            hic_r_str = ','.join(list_metadata[working_assembly]['HiC_reverse_reads'])
            if str(infos.at[i, 'HiC_reverse_reads']) != hic_r_str:
                infos.at[i, 'HiC_reverse_reads'] = hic_r_str
                changes_count += 1

        if 'HiC_Type' in list_metadata[working_assembly]:
            hic_type = list_metadata[working_assembly]['HiC_Type']
            if str(infos.at[i, 'HiC_Type']) != hic_type:
                infos.at[i, 'HiC_Type'] = hic_type
                changes_count += 1

        # Update invocations
        invocation_map = {
            'Workflow_1': 'Invocation_wf1',
            'Workflow_0': 'Invocation_wf0',
            'Workflow_4': 'Invocation_wf4',
            'Workflow_8_hap1': 'Invocation_wf8_hap1',
            'Workflow_8_hap2': 'Invocation_wf8_hap2',
            'Workflow_9_hap1': 'Invocation_wf9_hap1',
            'Workflow_9_hap2': 'Invocation_wf9_hap2'
        }

        if 'invocations' in list_metadata[working_assembly]:
            for wf_key, col_name in invocation_map.items():
                if wf_key in list_metadata[working_assembly]['invocations']:
                    inv_value = list_metadata[working_assembly]['invocations'][wf_key]
                    if inv_value and inv_value != 'NA':
                        if col_name not in infos.columns:
                            infos[col_name] = 'NA'
                            columns_added.append(col_name)
                        if str(infos.at[i, col_name]) != inv_value:
                            infos.at[i, col_name] = inv_value
                            changes_count += 1

    # Save tracking table
    infos.to_csv(track_table, sep='\t', header=True, index=False)

    print(f"\n{'='*60}")
    print(f"✓ Tracking table updated:")
    print(f"  - {changes_count} fields updated")
    if columns_added:
        print(f"  - {len(set(columns_added))} new columns added: {', '.join(set(columns_added))}")
    print(f"  Saved: {track_table}")
    print(f"{'='*60}\n")


def fetch_genomeark_urls(input_table):
    """Fetch GenomeArk URLs and create tracking table"""
    print(f"{'='*60}")
    print("Fetching GenomeArk URLs")
    print(f"{'='*60}\n")

    # Read input table without headers
    infos = pandas.read_csv(input_table, header=None, sep="\t")

    list_hifi_urls = []
    list_hic_type = []
    list_hic_f_urls = []
    list_hic_r_urls = []

    # Check columns: Species, Assembly, [Custom_Path], [Suffix]
    if len(infos.columns) == 4:
        infos.rename(columns={0: 'Species', 1: 'Assembly', 2: 'Custom_Path', 3: 'Suffix'}, inplace=True)
        has_custom_path = True
        has_suffix = True
        print("Detected 4 columns (Species, Assembly, Custom_Path, Suffix)")
    elif len(infos.columns) == 3:
        infos.rename(columns={0: 'Species', 1: 'Assembly', 2: 'Custom_Path'}, inplace=True)
        has_custom_path = True
        has_suffix = False
        print("Detected 3 columns (Species, Assembly, Custom_Path)")
    elif len(infos.columns) == 2:
        infos.rename(columns={0: 'Species', 1: 'Assembly'}, inplace=True)
        has_custom_path = False
        has_suffix = False
        print("Detected 2 columns (Species, Assembly)")
    else:
        raise SystemExit(f"Error: Input table must have 2, 3, or 4 columns (Species, Assembly, [Custom_Path], [Suffix]). Found {len(infos.columns)} columns.")

    print()
    for i, row in infos.iterrows():
        # Strip whitespace from all string columns
        species_name = str(row['Species']).strip()
        species_id = str(row['Assembly']).strip()

        # Get custom path if available and not empty
        custom_path = None
        if has_custom_path:
            cp_value = row['Custom_Path']
            if not pandas.isna(cp_value):
                cp_stripped = str(cp_value).strip()
                if cp_stripped:
                    custom_path = cp_stripped

        # Get suffix if available
        suffix = None
        if has_suffix:
            suffix_value = row['Suffix']
            if not pandas.isna(suffix_value):
                suffix_stripped = str(suffix_value).strip()
                if suffix_stripped:
                    suffix = suffix_stripped

        display_id = f"{species_id}_{suffix}" if suffix else species_id
        print(f"Fetching URLs for {display_id} ({species_name})...")
        hifi_reads, hic_type, hic_forward, hic_reverse = get_urls(species_name, species_id, custom_path)
        list_hifi_urls.append(hifi_reads)
        list_hic_type.append(hic_type)
        list_hic_f_urls.append(hic_forward)
        list_hic_r_urls.append(hic_reverse)

    # Add missing columns if they weren't in the input
    if not has_custom_path:
        infos['Custom_Path'] = ''
    if not has_suffix:
        infos['Suffix'] = ''

    # Create Working_Assembly column (used as unique key in metadata)
    def make_working_assembly(row):
        assembly = str(row['Assembly']).strip()
        if 'Suffix' in row and row['Suffix']:
            suffix = str(row['Suffix']).strip()
            if suffix:
                return f"{assembly}_{suffix}"
        return assembly
    infos['Working_Assembly'] = infos.apply(make_working_assembly, axis=1)

    infos['Hifi_reads'] = list_hifi_urls
    infos['HiC_Type'] = list_hic_type
    infos['HiC_forward_reads'] = list_hic_f_urls
    infos['HiC_reverse_reads'] = list_hic_r_urls

    # Save tracking table
    output_table = f"tracking_runs_{os.path.basename(input_table)}"
    infos.to_csv(output_table, sep='\t', header=True, index=False)

    print(f"\n✓ Created tracking table: {output_table}")
    print(f"  Species processed: {len(infos)}")
    print(f"  Columns: {', '.join(infos.columns)}\n")

    return output_table


def main():
    parser = argparse.ArgumentParser(
        prog='prepare_single',
        description='Prepare job files and commands for VGP workflows, or fetch GenomeArk URLs',
        usage='prepare_single.py --workflow N -t <table.tsv> -p <profile.yaml> [OPTIONS]\n'
              '       prepare_single.py --fetch_urls -t <species.tsv>',
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=textwrap.dedent('''
            Workflows:
              1          - Kmer profiling with HiFi data (VGP1)
              0          - Mitochondrial genome assembly (VGP0) [requires WF4]
              4          - Assembly with HiFi and Hi-C phasing (VGP4) [requires WF1]
              8          - Haplotype-specific assembly (VGP8) [requires WF4]
              9          - Decontamination (VGP9) [requires WF8]
              precuration - PretextMap generation for manual curation [requires WF4 + WF9 hap1 + WF9 hap2]

            Examples:
              # Fetch GenomeArk URLs (creates tracking_runs_species.tsv)
              prepare_single.py --fetch_urls -t species.tsv

              # Prepare workflows using profile file
              prepare_single.py --workflow 1 -t tracking_runs_species.tsv -p profile.yaml
              prepare_single.py --workflow 4 -t tracking_runs_species.tsv -p profile.yaml

              # Haplotype workflows
              prepare_single.py --workflow 8 -t tracking_runs_species.tsv -p profile.yaml -1
              prepare_single.py --workflow 9 -t tracking_runs_species.tsv -p profile.yaml -f -2

              # Pre-curation workflow (after WF4 and both WF9 haplotypes complete)
              prepare_single.py --workflow precuration -t tracking_runs_species.tsv -p profile.yaml

              # Sync metadata with tracking table (use after manual edits)
              prepare_single.py --update_metadata -t tracking_runs_species.tsv -p profile.yaml -m ./metadata/
              prepare_single.py --update_table -t tracking_runs_species.tsv -p profile.yaml -m ./metadata/
        '''))

    # Mode selection
    mode_group = parser.add_argument_group('Mode')
    mode_group.add_argument('--fetch_urls', action='store_true',
                            help='Fetch GenomeArk URLs and create tracking table')
    mode_group.add_argument('--workflow', '-w', dest='workflow',
                            choices=['1', '0', '4', '8', '9', 'precuration'],
                            help='Workflow to prepare (1, 0, 4, 8, 9, or precuration)')
    mode_group.add_argument('--update_metadata', action='store_true',
                            help='Update metadata from tracking table (table values take precedence)')
    mode_group.add_argument('--update_table', action='store_true',
                            help='Update tracking table from metadata (metadata values take precedence)')

    # Required for all modes
    parser.add_argument('-t', '--table', dest="track_table", required=True,
                        help='Input table (species.tsv for --fetch_urls, tracking_runs_*.tsv for --workflow)')

    # Profile file (required for workflow mode)
    parser.add_argument('-p', '--profile', dest="profile",
                        help='Path to profile YAML file (required for --workflow, contains Galaxy_instance, Galaxy_key, and Workflow_N values)')

    # Optional arguments
    parser.add_argument('-s', '--suffix', dest="suffix", default="",
                        help='Optional suffix for run naming')
    parser.add_argument('-m', '--metadata_directory', dest="metadata_dir", default="./",
                        help='Path to directory for run metadata (default: ./)')

    # Haplotype selection (for WF8 and WF9)
    haplotype_group = parser.add_argument_group('Haplotype selection (for workflows 8 and 9)')
    haplotype_group.add_argument('-1', '--hap1', action='store_true',
                                  help='Process haplotype 1')
    haplotype_group.add_argument('-2', '--hap2', action='store_true',
                                  help='Process haplotype 2')
    haplotype_group.add_argument('--paternal', action='store_true',
                                  help='Process paternal haplotype')
    haplotype_group.add_argument('--maternal', action='store_true',
                                  help='Process maternal haplotype')

    # Mode selection (for WF9)
    mode_group = parser.add_argument_group('Mode selection (for workflow 9 only)')
    mode_group.add_argument('-f', '--fcs', action='store_true',
                            help='Use NCBI FCS-GX mode (default)')
    mode_group.add_argument('-l', '--legacy', action='store_true',
                            help='Use Kraken2 legacy mode')

    args = parser.parse_args()

    # Validate mode selection (mutually exclusive)
    modes_selected = sum([args.fetch_urls, bool(args.workflow), args.update_metadata, args.update_table])
    if modes_selected == 0:
        raise SystemExit("Error: Please specify a mode: --fetch_urls, --workflow, --update_metadata, or --update_table")
    if modes_selected > 1:
        raise SystemExit("Error: Please select only ONE mode: --fetch_urls, --workflow, --update_metadata, or --update_table")

    # Handle fetch_urls mode
    if args.fetch_urls:
        fetch_genomeark_urls(args.track_table)
        return

    # Handle update modes
    if args.update_metadata or args.update_table:
        # Require profile for Galaxy connection
        if not args.profile:
            raise SystemExit("Error: -p/--profile is required for update modes")

        # Load profile using shared function
        profile_data = metadata.load_profile(args.profile)

        # Setup Galaxy connection using shared function
        gi, galaxy_instance = metadata.setup_galaxy_connection(profile_data)

        # Setup metadata directory
        metadata_dir = utils.fix_directory(args.metadata_dir)
        if not os.path.exists(metadata_dir):
            os.makedirs(metadata_dir)

        # Process suffix
        suffix_run, _ = utils.fix_parameters(args.suffix, galaxy_instance)

        if args.update_metadata:
            update_metadata_from_table(args.track_table, metadata_dir, suffix_run, gi)
        else:  # args.update_table
            update_table_from_metadata(args.track_table, metadata_dir, suffix_run)

        return

    # Workflow preparation mode - requires profile
    if not args.profile:
        raise SystemExit("Error: -p/--profile is required for workflow preparation")

    wf_num = args.workflow
    config = WORKFLOW_CONFIGS[wf_num]

    # Initialize variables
    path_script = str(pathlib.Path(__file__).parent.parent / "batch_vgp_run")

    # Load profile using shared function
    profile_data = metadata.load_profile(args.profile)

    # Extract workflow specification from profile
    workflow_key = f'Workflow_{wf_num}'
    if workflow_key not in profile_data:
        raise SystemExit(f"Error: Profile file missing '{workflow_key}' field")

    workflow_value = profile_data[workflow_key]

    # Setup Galaxy connection using shared function
    gi, galaxy_instance = metadata.setup_galaxy_connection(profile_data)

    # Resolve workflow (auto-detect ID vs version)
    wfl_dir = utils.fix_directory(path_script + "/workflows/")
    workflow_path, release_number, _ = workflow_manager.resolve_workflow(
        gi,
        workflow_value,
        config['name'],
        wfl_dir
    )

    # If version was uploaded to Galaxy, update profile
    if release_number is not None:
        profile_data[workflow_key] = workflow_path

        print("\n" + "="*60)
        print("Workflow was uploaded to Galaxy")
        print(f"  Workflow: {config['label']} (version {release_number})")
        print(f"  Galaxy ID: {workflow_path}")

        # Create backup and save updated profile
        profile_backup = args.profile + ".bak"
        shutil.copy2(args.profile, profile_backup)
        print(f"  Backup: {profile_backup}")

        with open(args.profile, 'w') as f:
            yaml.dump(profile_data, f, default_flow_style=False, sort_keys=False)
        print(f"  Updated profile: {args.profile}")
        print("="*60 + "\n")

    print(f"Using profile: {args.profile}")
    print(f"  Galaxy: {galaxy_instance}")
    print(f"  Workflow: {config['label']}")
    if release_number:
        print(f"  Version: {release_number}")
    print()

    # Setup metadata directory
    metadata_dir = utils.fix_directory(args.metadata_dir)
    if not os.path.exists(metadata_dir):
        os.makedirs(metadata_dir)
        print(f"Created metadata directory: {metadata_dir}")

    # Process suffix
    suffix_run, _ = utils.fix_parameters(args.suffix, galaxy_instance)

    # Load or initialize metadata using shared function
    list_metadata, dico_workflows = metadata.load_metadata(metadata_dir, suffix_run)

    if list_metadata:
        print(f"Loading existing metadata from: {metadata_dir}")
    else:
        print(f"Creating new metadata")

    # Initialize workflow metadata if it doesn't exist
    if not dico_workflows:
        # Initialize workflow metadata structure
        dico_workflows = {
            "Workflow_1": {"Name": "kmer-profiling-hifi-VGP1", "Path": workflow_path if wf_num == '1' else 'NA', "version": release_number if wf_num == '1' and release_number else 'NA'},
            "Workflow_0": {"Name": "Mitogenome-assembly-VGP0", "Path": workflow_path if wf_num == '0' else 'NA', "version": release_number if wf_num == '0' and release_number else 'NA'},
            "Workflow_4": {"Name": "Assembly-Hifi-HiC-phasing-VGP4", "Path": workflow_path if wf_num == '4' else 'NA', "version": release_number if wf_num == '4' and release_number else 'NA'},
            "Workflow_8_hap1": {"Name": "Scaffolding-HiC-VGP8", "Path": workflow_path if wf_num == '8' else 'NA', "version": release_number if wf_num == '8' and release_number else 'NA'},
            "Workflow_8_hap2": {"Name": "Scaffolding-HiC-VGP8", "Path": workflow_path if wf_num == '8' else 'NA', "version": release_number if wf_num == '8' and release_number else 'NA'},
            "Workflow_9_hap1": {"Name": "Assembly-decontamination-VGP9", "Path": workflow_path if wf_num == '9' else 'NA', "version": release_number if wf_num == '9' and release_number else 'NA'},
            "Workflow_9_hap2": {"Name": "Assembly-decontamination-VGP9", "Path": workflow_path if wf_num == '9' else 'NA', "version": release_number if wf_num == '9' and release_number else 'NA'},
        }

    # Update workflow metadata for current workflow
    if wf_num == '1':
        dico_workflows["Workflow_1"]["Path"] = workflow_path
        if release_number:
            dico_workflows["Workflow_1"]["version"] = release_number
    elif wf_num == '0':
        dico_workflows["Workflow_0"]["Path"] = workflow_path
        if release_number:
            dico_workflows["Workflow_0"]["version"] = release_number
    elif wf_num == '4':
        dico_workflows["Workflow_4"]["Path"] = workflow_path
        if release_number:
            dico_workflows["Workflow_4"]["version"] = release_number
    elif wf_num == '8':
        dico_workflows["Workflow_8_hap1"]["Path"] = workflow_path
        dico_workflows["Workflow_8_hap2"]["Path"] = workflow_path
        if release_number:
            dico_workflows["Workflow_8_hap1"]["version"] = release_number
            dico_workflows["Workflow_8_hap2"]["version"] = release_number
    elif wf_num == '9':
        dico_workflows["Workflow_9_hap1"]["Path"] = workflow_path
        dico_workflows["Workflow_9_hap2"]["Path"] = workflow_path
        if release_number:
            dico_workflows["Workflow_9_hap1"]["version"] = release_number
            dico_workflows["Workflow_9_hap2"]["version"] = release_number
    elif wf_num == 'precuration':
        # Initialize PreCuration workflow in dico_workflows if not present
        if "Workflow_PreCuration" not in dico_workflows:
            dico_workflows["Workflow_PreCuration"] = {"Name": "PretextMap-Generation"}
        dico_workflows["Workflow_PreCuration"]["Path"] = workflow_path
        if release_number:
            dico_workflows["Workflow_PreCuration"]["version"] = release_number

    # Check if workflow requires haplotype selection
    hap_suffix = None
    if config.get('has_haplotypes'):
        selected_haps = sum([args.hap1, args.hap2, args.paternal, args.maternal])
        if selected_haps == 0:
            raise SystemExit(f"Error: Workflow {wf_num} requires haplotype selection (-1, -2, -p, or -m)")
        if selected_haps > 1:
            raise SystemExit(f"Error: Please select only one haplotype")

        if args.hap1:
            hap_suffix = "hap1"
        elif args.hap2:
            hap_suffix = "hap2"
        elif args.paternal:
            hap_suffix = "pat"
        elif args.maternal:
            hap_suffix = "mat"
    else:
        if any([args.hap1, args.hap2, args.paternal, args.maternal]):
            print(f"Warning: Haplotype flags are not used for Workflow {wf_num}")

    # Check mode selection for WF9
    template_file = None
    if wf_num == '9':
        if args.legacy:
            template_file = config['template_legacy']
            print(f"Using decontamination mode: Kraken2 legacy")
        else:
            template_file = config['template_fcs']
            print(f"Using decontamination mode: NCBI FCS-GX")
    else:
        template_file = config.get('template')
        if args.fcs or args.legacy:
            print(f"Warning: Mode flags (-f/-l) are only used for Workflow 9")

    # Read tracking table
    infos = pandas.read_csv(args.track_table, header=0, sep="\t")

    # Merge metadata into tracking table (metadata takes precedence)
    print("\nMerging metadata with tracking table...")
    print("  (Metadata takes precedence when both sources have data)\n")
    species_from_metadata = 0
    species_from_table = 0
    for i, row in infos.iterrows():
        spec_name = str(row['Species']).strip()
        assembly_id = str(row['Assembly']).strip()
        working_assembly = utils.get_working_assembly(row, infos, i)

        # Initialize metadata for this species if not present
        if working_assembly not in list_metadata:
            print(f"  {working_assembly}: Initializing from tracking table")
            species_from_table += 1
            list_metadata[working_assembly] = {
                'Assembly': assembly_id,
                'History_name': working_assembly + suffix_run,
                'Name': spec_name,
                'Custom_Path': str(infos.iloc[i]['Custom_Path']).strip() if 'Custom_Path' in infos.columns and pandas.notna(infos.iloc[i]['Custom_Path']) else '',
                'Path': f"./{working_assembly}/",
                'job_files': {},
                'invocation_jsons': {},
                'planemo_logs': {},
                'reports': {},
                'invocations': {},
                'dataset_ids': {},
                'history_id': 'NA',
                'taxon_id': 'NA',
                'failed_invocations': {}
            }

            # Add genomic data from tracking table
            if 'Hifi_reads' in infos.columns:
                hifi_col = infos.iloc[i]['Hifi_reads']
                if hifi_col != 'NA' and pandas.notna(hifi_col):
                    list_metadata[working_assembly]['Hifi_reads'] = hifi_col.split(',')
                else:
                    list_metadata[working_assembly]['Hifi_reads'] = []

            if 'HiC_forward_reads' in infos.columns and 'HiC_reverse_reads' in infos.columns:
                hic_f_col = infos.iloc[i]['HiC_forward_reads']
                hic_r_col = infos.iloc[i]['HiC_reverse_reads']
                if pandas.notna(hic_f_col) and pandas.notna(hic_r_col):
                    list_metadata[working_assembly]['HiC_forward_reads'] = hic_f_col.split(',')
                    list_metadata[working_assembly]['HiC_reverse_reads'] = hic_r_col.split(',')
                else:
                    list_metadata[working_assembly]['HiC_forward_reads'] = []
                    list_metadata[working_assembly]['HiC_reverse_reads'] = []

            if 'HiC_Type' in infos.columns:
                hic_type = infos.iloc[i]['HiC_Type']
                if pandas.notna(hic_type):
                    list_metadata[working_assembly]['HiC_Type'] = hic_type
        else:
            print(f"  {working_assembly}: Using existing metadata")
            species_from_metadata += 1
            # Update tracking table from metadata (metadata takes precedence)

            # Ensure tracking table has required columns
            if 'Hifi_reads' not in infos.columns:
                infos['Hifi_reads'] = 'NA'
            if 'HiC_forward_reads' not in infos.columns:
                infos['HiC_forward_reads'] = 'NA'
            if 'HiC_reverse_reads' not in infos.columns:
                infos['HiC_reverse_reads'] = 'NA'
            if 'HiC_Type' not in infos.columns:
                infos['HiC_Type'] = 'NA'

            # Update Hifi_reads from metadata
            if 'Hifi_reads' in list_metadata[working_assembly] and list_metadata[working_assembly]['Hifi_reads']:
                hifi_str = ','.join(list_metadata[working_assembly]['Hifi_reads'])
                infos.at[i, 'Hifi_reads'] = hifi_str
            elif pandas.isna(infos.at[i, 'Hifi_reads']) or infos.at[i, 'Hifi_reads'] == '':
                # If metadata doesn't have it and tracking table doesn't have it, mark as NA
                infos.at[i, 'Hifi_reads'] = 'NA'

            # Update HiC reads from metadata
            if 'HiC_forward_reads' in list_metadata[working_assembly] and list_metadata[working_assembly]['HiC_forward_reads']:
                hic_f_str = ','.join(list_metadata[working_assembly]['HiC_forward_reads'])
                infos.at[i, 'HiC_forward_reads'] = hic_f_str
            elif pandas.isna(infos.at[i, 'HiC_forward_reads']) or infos.at[i, 'HiC_forward_reads'] == '':
                infos.at[i, 'HiC_forward_reads'] = 'NA'

            if 'HiC_reverse_reads' in list_metadata[working_assembly] and list_metadata[working_assembly]['HiC_reverse_reads']:
                hic_r_str = ','.join(list_metadata[working_assembly]['HiC_reverse_reads'])
                infos.at[i, 'HiC_reverse_reads'] = hic_r_str
            elif pandas.isna(infos.at[i, 'HiC_reverse_reads']) or infos.at[i, 'HiC_reverse_reads'] == '':
                infos.at[i, 'HiC_reverse_reads'] = 'NA'

            # Update HiC_Type from metadata
            if 'HiC_Type' in list_metadata[working_assembly]:
                infos.at[i, 'HiC_Type'] = list_metadata[working_assembly]['HiC_Type']
            elif pandas.isna(infos.at[i, 'HiC_Type']) or infos.at[i, 'HiC_Type'] == '':
                infos.at[i, 'HiC_Type'] = 'NA'

            # Update invocations if they exist in metadata
            for wf_key in ['Workflow_1', 'Workflow_0', 'Workflow_4', 'Workflow_8_hap1', 'Workflow_8_hap2', 'Workflow_9_hap1', 'Workflow_9_hap2']:
                if 'invocations' in list_metadata[working_assembly] and wf_key in list_metadata[working_assembly]['invocations']:
                    inv_value = list_metadata[working_assembly]['invocations'][wf_key]
                    if inv_value and inv_value != 'NA':
                        # Map workflow keys to tracking table column names
                        col_map = {
                            'Workflow_1': 'Invocation_wf1',
                            'Workflow_0': 'Invocation_wf0',
                            'Workflow_4': 'Invocation_wf4',
                            'Workflow_8_hap1': 'Invocation_wf8_hap1',
                            'Workflow_8_hap2': 'Invocation_wf8_hap2',
                            'Workflow_9_hap1': 'Invocation_wf9_hap1',
                            'Workflow_9_hap2': 'Invocation_wf9_hap2'
                        }
                        col_name = col_map.get(wf_key)
                        if col_name:
                            if col_name not in infos.columns:
                                infos[col_name] = 'NA'
                            infos.at[i, col_name] = inv_value

    print(f"\n✓ Data sources:")
    print(f"  - {species_from_metadata} species loaded from existing metadata")
    print(f"  - {species_from_table} species initialized from tracking table")
    print(f"  Total: {len(list_metadata)} species\n")

    # Check for required columns based on workflow dependencies
    if config['requires'] and config['invocation_column']:
        inv_col = config['invocation_column'].replace('{hap}', hap_suffix) if hap_suffix else config['invocation_column']
        if inv_col not in infos.columns:
            raise SystemExit(f"Error: Workflow {wf_num} requires '{inv_col}' column. "
                           f"Please run Workflow {config['requires']} first.")

    print(f"\n{'='*60}")
    print(f"Preparing {config['description']}")
    if hap_suffix:
        print(f"Haplotype: {hap_suffix}")
    print(f"{'='*60}\n")

    # Prepare workflow-specific job files
    # Metadata is updated directly in prepare functions
    if wf_num == '1':
        commands = prepare_workflow_1(infos, gi, workflow_path, galaxy_instance, suffix_run, path_script, list_metadata)
    elif wf_num == '4':
        commands = prepare_workflow_4(infos, gi, workflow_path, galaxy_instance, suffix_run, path_script, list_metadata)
    elif wf_num == '8':
        commands = prepare_workflow_8(infos, gi, workflow_path, galaxy_instance, suffix_run, path_script, hap_suffix, list_metadata)
    elif wf_num == '9':
        commands = prepare_workflow_9(infos, gi, workflow_path, galaxy_instance, suffix_run, path_script, hap_suffix, template_file, list_metadata)
    elif wf_num == '0':
        commands = prepare_workflow_0(infos, gi, workflow_path, galaxy_instance, suffix_run, path_script, list_metadata)
    elif wf_num == 'precuration':
        commands = prepare_precuration(infos, gi, workflow_path, galaxy_instance, suffix_run, path_script, list_metadata)

    # Save metadata files using shared function
    print(f"\nSaving metadata...")
    metadata.save_metadata(metadata_dir, list_metadata, suffix_run, dico_workflows)
    print(f"✓ Saved metadata to: {metadata_dir}")

    # Print commands
    # All workflows are now refactored - commands list only contains actual commands (no "NA")
    print(f"\n✓ Prepared {len(commands)} job files")
    print(f"{'='*60}\n")

    if commands:
        print("Commands to run:")
        for cmd in commands:
            print(cmd)
        print()


if __name__ == "__main__":
    main()
