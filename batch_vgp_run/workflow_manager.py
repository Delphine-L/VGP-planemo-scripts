#!/usr/bin/env python3

"""
Workflow management functions for VGP pipeline.

This module handles:
- Workflow download from GitHub (iwc-workflows)
- Workflow version detection and resolution
- Workflow upload to Galaxy
- Workflow ID vs version auto-detection
"""

import os
import re
import json
import zipfile
import shutil
from batch_vgp_run.utils import download_file
from batch_vgp_run.logging_utils import log_warning


def get_workflow_version(path_ga):
    """
    Extract release/version number from a workflow .ga file.

    Args:
        path_ga (str): Path to the workflow .ga file

    Returns:
        str: Version/release number, or 'NA' if not found
    """
    try:
        with open(path_ga) as wfjson:
            gawf = json.load(wfjson)
            if 'release' in gawf.keys():
                release_number = gawf['release']
            else:
                release_number = 'NA'
        return release_number
    except FileNotFoundError:
        print(f"Error: File not found at {path_ga}")
        return 'NA'


def get_worfklow(Compatible_version, workflow_name, workflow_repo):
    """
    Download workflow from iwc-workflows GitHub repository.

    Note: Function name has typo (worfklow) preserved from original for compatibility.

    Args:
        Compatible_version (str): Version number (e.g., "0.5")
        workflow_name (str): Name of the workflow
        workflow_repo (str): Directory to store workflow files

    Returns:
        tuple: (file_path, release_number)
    """
    os.makedirs(workflow_repo, exist_ok=True)
    url_workflow = f"https://github.com/iwc-workflows/{workflow_name}/archive/refs/tags/v{Compatible_version}.zip"
    path_compatible = f"{workflow_name}-{Compatible_version}/{workflow_name}.ga"
    file_path = f"{workflow_repo}{workflow_name}.ga"
    archive_path = f"{workflow_repo}{workflow_name}.zip"

    if os.path.exists(file_path):
        print(f'Workflow {workflow_name} found.\n')
        release_number = get_workflow_version(file_path)
    else:
        release_number = Compatible_version
        download_file(url_workflow, archive_path)
        with zipfile.ZipFile(archive_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            for item in file_list:
                if f"{workflow_name.lower()}.ga" in item.lower():
                    extracted_path = item
                    zip_ref.extract(path=workflow_repo, member=item)
            os.remove(archive_path)
            shutil.move(f"{workflow_repo}{extracted_path}", file_path)

            # Add version to workflow name
            with open(file_path, 'r+') as wf_file:
                workflow_data = json.load(wf_file)
                workflow_data['name'] = f"{workflow_data.get('name', workflow_name)} - v{release_number}"
                wf_file.seek(0)
                json.dump(workflow_data, wf_file, indent=4)
                wf_file.truncate()

        os.rmdir(f"{workflow_repo}{workflow_name}-{Compatible_version}/")

    return file_path, release_number


def is_workflow_id(value):
    """
    Detect if a value is a Galaxy workflow ID or a version number.

    Galaxy workflow IDs are typically 16-character hexadecimal strings.
    Version numbers are in format X.Y or X.Y.Z (e.g., "0.5", "1.2.3").

    Args:
        value (str): The value to check

    Returns:
        bool: True if value appears to be a workflow ID, False if it looks like a version
    """
    # Check if it's a hex string (workflow ID pattern)
    if re.match(r'^[a-f0-9]{16}$', str(value)):
        return True

    # Check if it's a version number pattern (X.Y or X.Y.Z)
    if re.match(r'^\d+\.\d+(\.\d+)?$', str(value)):
        return False

    # If neither pattern matches clearly, try to determine based on length and characters
    # Workflow IDs are 16 chars and contain only hex digits
    value_str = str(value)
    if len(value_str) == 16 and all(c in '0123456789abcdef' for c in value_str.lower()):
        return True

    # Default to assuming it's a version if it contains dots
    if '.' in value_str:
        return False

    # If still unclear, default to version (safer for backward compatibility)
    log_warning(f"Could not clearly determine if '{value}' is a workflow ID or version. Treating as version.")
    return False


def upload_workflow_to_galaxy(gi, workflow_file_path):
    """
    Upload a workflow file to Galaxy and return its ID.

    Args:
        gi (GalaxyInstance): Connected Galaxy instance
        workflow_file_path (str): Path to the workflow .ga file

    Returns:
        str: The workflow ID assigned by Galaxy

    Raises:
        Exception: If upload fails
    """
    try:
        with open(workflow_file_path, 'r') as wf_file:
            workflow_dict = json.load(wf_file)

        # Import workflow to Galaxy
        result = gi.workflows.import_workflow_dict(workflow_dict)
        workflow_id = result['id']
        workflow_name = result.get('name', 'Unknown')

        print(f"✓ Uploaded workflow '{workflow_name}' to Galaxy (ID: {workflow_id})")
        return workflow_id

    except FileNotFoundError:
        raise Exception(f"Workflow file not found: {workflow_file_path}")
    except json.JSONDecodeError:
        raise Exception(f"Invalid JSON in workflow file: {workflow_file_path}")
    except Exception as e:
        raise Exception(f"Failed to upload workflow to Galaxy: {e}")


def resolve_workflow(gi, workflow_value, workflow_name, workflow_repo):
    """
    Resolve a workflow specification to a Galaxy workflow ID.

    This function handles both workflow IDs and version numbers:
    - If the value is already a workflow ID, returns it directly
    - If the value is a version number, downloads the workflow, uploads it to Galaxy,
      and returns the new workflow ID

    Args:
        gi (GalaxyInstance): Connected Galaxy instance
        workflow_value (str): Either a workflow ID or version number
        workflow_name (str): Name of the workflow (for downloading from GitHub)
        workflow_repo (str): Directory to store downloaded workflows

    Returns:
        tuple: (workflow_id, version_number, workflow_path)
            - workflow_id: The Galaxy workflow ID to use with planemo
            - version_number: The version number (from file or input)
            - workflow_path: Local path to the workflow file
    """
    if is_workflow_id(workflow_value):
        # It's already an ID, use it directly
        print(f"Using existing workflow ID: {workflow_value}")
        return workflow_value, None, None
    else:
        # It's a version number, download and upload
        print(f"Downloading workflow {workflow_name} version {workflow_value}...")
        workflow_path, version_number = get_worfklow(workflow_value, workflow_name, workflow_repo)

        print(f"Uploading workflow to Galaxy...")
        workflow_id = upload_workflow_to_galaxy(gi, workflow_path)

        return workflow_id, version_number, workflow_path
