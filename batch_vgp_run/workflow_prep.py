#!/usr/bin/env python3

"""
Workflow YAML preparation functions for VGP pipeline.

This module contains functions for generating YAML job files for each workflow:
- Workflow 0: Mitochondrial assembly (VGP0)
- Workflow 4: Assembly with HiFi and Hi-C phasing (VGP4)
- Workflow 8: Haplotype-specific scaffolding (VGP8)
- Workflow 9: Decontamination (VGP9)
- Pre-curation: PretextMap generation
"""

import re
import os
import subprocess
from bioblend.galaxy import GalaxyInstance
from batch_vgp_run.galaxy_client import get_datasets_ids


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

