#!/usr/bin/env python3


import os
import requests
import zipfile

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
        print(f"Error downloading file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def get_worfklow(Compatible_workflow, path_compatible, archive_name):

    file_path = "workflows/"+path_compatible
    worfklow_name=path_compatible.split('/')[-1]
    if os.path.exists(worfklow_name):
        print(path_compatible+" found.\n")
    else:
        os.makedirs("workflows/", exist_ok=True)
        download_file(Compatible_workflow, "workflows/"+archive_name)
        with zipfile.ZipFile("workflows/"+archive_name, 'r') as zip_ref:
            #print(zip_ref.namelist())
            zip_ref.extract(path='workflows/', member=path_compatible) 
            os.remove('workflows/'+archive_name)
        os.symlink('workflows/'+path_compatible, worfklow_name)

    return worfklow_name