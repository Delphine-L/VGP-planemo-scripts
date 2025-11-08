#!/usr/bin/env python3

"""
General utility functions for VGP pipeline.

This module contains utilities for:
- Path and URL normalization
- Data extraction from dataframes
- File operations
- Dictionary utilities
"""

import os
import re
import requests
import pandas
from collections import defaultdict


def normalize_suffix(suffix):
    """
    Normalize a suffix value, returning empty string if it's NA/NaN/empty.

    This prevents suffixes like "_nan" or "_NA" from being appended to filenames.

    Args:
        suffix: The suffix value (any type)

    Returns:
        str: Normalized suffix (empty string if invalid, otherwise the suffix as-is)
    """
    # Check for pandas NaN
    if pandas.isna(suffix):
        return ""

    # Convert to string and check
    str_value = str(suffix).strip()

    # Check for various NA representations
    if str_value.lower() in ['na', 'nan', 'none', '']:
        return ""

    return str_value


def fix_parameters(suffix, galaxy_url):
    """
    Normalize suffix and Galaxy URL parameters.

    Args:
        suffix (str): Optional suffix for run naming
        galaxy_url (str): Galaxy instance URL

    Returns:
        tuple: (normalized_suffix, normalized_url)
    """
    # First normalize to handle NaN/NA values
    suffix = normalize_suffix(suffix)

    suffix_run = ""
    if suffix and suffix.strip():
        suffix_value = suffix.strip()
        if not suffix_value.startswith('_'):
            suffix_run = '_' + suffix_value
        else:
            suffix_run = suffix_value

    # Normalize Galaxy URL
    if not galaxy_url.startswith('http'):
        galaxy_url = 'https://' + galaxy_url

    return suffix_run, galaxy_url


def fix_directory(path):
    """
    Ensure directory path ends with a trailing slash.

    Args:
        path (str): Directory path

    Returns:
        str: Normalized path with trailing slash
    """
    if not path.endswith('/'):
        return path + '/'
    return path


def find_duplicate_values(input_dict):
    """
    Find values in a dictionary that are associated with multiple keys.

    Args:
        input_dict (dict): The dictionary to search for duplicate values

    Returns:
        dict: A dictionary where keys are the duplicate values and values are
              lists of keys that share that value
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
    Download a file from a given URL and save it to a specified path.

    Args:
        url (str): The URL of the file to download
        save_path (str): The local path to save the downloaded file
    """
    try:
        response = requests.get(url, stream=True)  # Use stream=True for large files
        response.raise_for_status()  # Raise an exception for bad status codes

        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"File downloaded successfully to: {save_path}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        raise


def get_working_assembly(row, infos=None, index=None):
    """
    Extract working assembly ID from a dataframe row.

    The working assembly is the unique identifier used throughout the pipeline.
    It's either the Working_Assembly column value, or falls back to Assembly.

    Args:
        row: DataFrame row (can be Series from iterrows or direct row access)
        infos: Optional DataFrame (for column checking)
        index: Optional row index (for iloc access)

    Returns:
        str: Working assembly ID (may include suffix like "assembly_somatic")
    """
    # Get assembly_id first (always present)
    if index is not None and infos is not None:
        assembly_id = str(infos.iloc[index]['Assembly']).strip()
    else:
        assembly_id = str(row['Assembly']).strip()

    # Check for Working_Assembly column
    has_working_assembly = False
    if infos is not None:
        has_working_assembly = 'Working_Assembly' in infos.columns
    elif hasattr(row, 'index'):  # Series from iterrows
        has_working_assembly = 'Working_Assembly' in row.index

    if not has_working_assembly:
        return assembly_id

    # Try to get Working_Assembly value
    if index is not None and infos is not None:
        wa_value = infos.iloc[index]['Working_Assembly']
    else:
        wa_value = row.get('Working_Assembly') if hasattr(row, 'get') else row['Working_Assembly']

    # Use Working_Assembly if it exists and is not empty
    if pandas.notna(wa_value) and str(wa_value).strip() != '':
        return str(wa_value).strip()

    return assembly_id


def get_custom_path_for_genomeark(row, assembly_id, infos=None, index=None):
    """
    Extract and format custom path for GenomeArk URLs.

    Custom paths allow handling non-standard GenomeArk directory structures
    (e.g., species/Genus_species/id/somatic/genomic_data/).

    Args:
        row: DataFrame row
        assembly_id: Assembly ID (used for path extraction)
        infos: Optional DataFrame
        index: Optional row index

    Returns:
        str: Formatted path segment (e.g., "/somatic" or "") for GenomeArk URLs
    """
    # Check if Custom_Path column exists
    has_custom_path = False
    if infos is not None:
        has_custom_path = 'Custom_Path' in infos.columns
    elif hasattr(row, 'index'):  # Series from iterrows
        has_custom_path = 'Custom_Path' in row.index

    if not has_custom_path:
        return ''

    # Get Custom_Path value
    if index is not None and infos is not None:
        cp_value = infos.iloc[index]['Custom_Path']
    else:
        cp_value = row.get('Custom_Path') if hasattr(row, 'get') else row['Custom_Path']

    # Check if value exists and is not empty
    if pandas.isna(cp_value) or str(cp_value).strip() == '':
        return ''

    full_path = str(cp_value).strip()

    # Extract subdirectory between assembly_id and genomic_data
    # Pattern: assembly_id/(SUBDIRECTORY)/genomic_data
    pattern = f"{assembly_id}/(.*?)/genomic_data"
    match = re.search(pattern, full_path)

    if match:
        # Return with leading slash for URL construction
        return '/' + match.group(1)

    return ''
