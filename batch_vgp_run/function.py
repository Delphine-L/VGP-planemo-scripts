#!/usr/bin/env python3


import os
import requests
import zipfile
import json
import shutil
from bioblend.galaxy import GalaxyInstance
import re
from collections import defaultdict

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