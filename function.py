#!/usr/bin/env python3


import os
import requests
import zipfile
import json
import shutil


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
            #print(zip_ref.namelist())
            zip_ref.extract(path=workflow_repo, member=path_compatible) 
            os.remove(archive_path)
        shutil.move(workflow_repo+path_compatible, file_path)
        os.rmdir(workflow_repo+workflow_name+"-"+Compatible_version+"/")
    return file_path, release_number



        
