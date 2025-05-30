####################################################################################################
#                                                                                                  #
#   Project: CNPJ Data Extractor                                                                   #
#   Description: This project extracts and processes the CNPJ (Brazilian tax ID) information       #
#                of companies from publicly available datasets. It automates the process of        #
#                extraction and transform data for further analysis.                               #
#                                                                                                  #
#   Created by: Joao M. Feck (GitHub: https://github.com/jmfeck)                                   #
#   Email: joaomfeck@gmail.com                                                                     #
#                                                                                                  #
####################################################################################################

import os
import logging
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import yaml
import re

########################## Load Configurations ##########################
data_incoming_foldername = 'data_incoming'
data_outgoing_foldername = 'data_outgoing'
log_foldername = 'logs'
log_filename = 'cnpj_extractor.log'
config_foldername = 'config'
config_filename = 'config.yaml'

path_script = os.path.abspath(__file__)
path_script_dir = os.path.dirname(path_script)
path_project = os.path.dirname(path_script_dir)
path_incoming = os.path.join(path_project, data_incoming_foldername)
path_outgoing = os.path.join(path_project, data_outgoing_foldername)
path_log_dir = os.path.join(path_project, log_foldername)
path_log = os.path.join(path_log_dir, log_filename)
path_config_dir = os.path.join(path_project, config_foldername)
path_config = os.path.join(path_config_dir, config_filename)

# Ensure outgoing and log folders exist
os.makedirs(path_log_dir, exist_ok=True)
os.makedirs(path_incoming, exist_ok=True)

with open(path_config, 'r') as file:
    config = yaml.safe_load(file)

# Root URL to fetch folders
base_url = config['base_url']

# Set up logging
logging.basicConfig(filename=path_log, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def get_remote_file_size(url):
    """Fetch the file size of the remote file from HTTP headers."""
    try:
        response = requests.head(url)
        response.raise_for_status()
        return int(response.headers.get('content-length', 0))  # Return the remote file size
    except requests.RequestException as e:
        logging.error(f"Failed to fetch file size for {url}: {e}")
        tqdm.write(f"Failed to fetch file size for {url}: {e}")
        return None


def get_latest_month_folder(url):
    """Fetch the folder list from the URL and return the latest (most recent) month folder."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Regex to match folders with the format YYYY-MM
        folder_pattern = re.compile(r'\b\d{4}-\d{2}\b')
        
        # Find all links to directories (usually ending with /) that match the format YYYY-MM
        directories = [a['href'].strip('/') for a in soup.find_all('a', href=True) if folder_pattern.match(a['href'])]
        # Sort directories to find the most recent one
        latest_folder = sorted(directories, reverse=True)[0]
        return latest_folder.strip('/')
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch website directories: {e}")
        tqdm.write(f"Failed to fetch website directories: {e}")
        return None


def get_all_files_in_folder(url):
    """Fetch all file links in the folder."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        # Find all links to files (not directories)
        files = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.zip')]
        return files
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch files from folder: {e}")
        tqdm.write(f"Failed to fetch files from folder: {e}")
        return []


def download_file(url, data_outgoing_folder=''):
    """Download a file from the given URL and save it to the specified folder, with progress bar."""
    local_file_name = url.split('/')[-1]
    local_file_path = os.path.join(data_outgoing_folder, local_file_name)

    # Fetch remote file size
    remote_file_size = get_remote_file_size(url)

    # Check if the file already exists and compare its size
    if os.path.exists(local_file_path):
        local_file_size = os.path.getsize(local_file_path)

        if remote_file_size and local_file_size == remote_file_size:
            logging.info(f"File {local_file_name} already exists and matches the size. Skipping download.")
            tqdm.write(f"File {local_file_name} already exists and matches the size. Skipping download.")
            return local_file_path
        else:
            logging.info(f"File {local_file_name} exists but size does not match. Re-downloading.")
            tqdm.write(f"File {local_file_name} exists but size does not match. Re-downloading.")
    
    # If the file doesn't exist or the sizes don't match, download the file
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))  # Total file size in bytes
            chunk_size = 8192  # Define chunk size (8 KB)
            
            with open(local_file_path, 'wb') as f:
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=local_file_name, ncols=80) as pbar:
                    for chunk in r.iter_content(chunk_size=chunk_size): 
                        f.write(chunk)
                        pbar.update(len(chunk))  # Update progress bar with chunk size
            
        logging.info(f"Downloaded {local_file_name} successfully.")
        tqdm.write(f"Downloaded {local_file_name} successfully.")
        return local_file_path
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download {local_file_name}: {e}")
        tqdm.write(f"Failed to download {local_file_name}: {e}")
        return None


def download_file_parallel(url):
    """Wrapper function to use in parallel download."""
    return download_file(url, path_incoming)


# Get the number of CPU cores available
def get_available_threads():
    try:
        return os.cpu_count()
    except Exception as e:
        logging.error(f"Error determining available threads: {e}")
        return 1  # Fallback to 1 thread if we can't determine


# Get the most recent month folder
latest_folder = get_latest_month_folder(base_url)

if latest_folder:
    logging.info(f"Latest month folder: {latest_folder}")
    tqdm.write(f"Latest month folder: {latest_folder}")
    # Build the full folder URL for the latest month
    folder_url = base_url + '/' + latest_folder + '/'
    
    # Get all files in the folder
    files_in_folder = get_all_files_in_folder(folder_url)
    
    if files_in_folder:
        logging.info(f"Found {len(files_in_folder)} files in folder {latest_folder}")
        tqdm.write(f"Found {len(files_in_folder)} files in folder {latest_folder}")
        
        # Create full download URLs for each file
        list_of_urls = [folder_url + file for file in files_in_folder]
        
        # Get available threads/CPU cores
        available_threads = get_available_threads()
        logging.info(f"Number of available threads (CPU cores): {available_threads}")
        tqdm.write(f"Number of available threads (CPU cores): {available_threads}")
        
        # Set up the ThreadPoolExecutor to download files in parallel
        with ThreadPoolExecutor(max_workers=available_threads) as executor:
            # Submit the download tasks to the executor
            futures = {executor.submit(download_file_parallel, url): url for url in list_of_urls}
            
            # Process each future as it completes
            for future in as_completed(futures):
                url = futures[future]
                try:
                    result = future.result()
                    logging.info(f"Downloaded: {url}")
                    tqdm.write(f"Downloaded: {url}")
                except Exception as e:
                    logging.error(f"Error downloading {url}: {e}")
                    tqdm.write(f"Error downloading {url}: {e}")

        # Optional: Print a summary of successful and failed downloads
        logging.info(f"Finished downloading files.")
        tqdm.write(f"Finished downloading files.")
    else:
        logging.info(f"No files found in the folder: {latest_folder}")
        tqdm.write(f"No files found in the folder: {latest_folder}")
else:
    logging.info("Could not find the latest month folder. Please check the URL or connection.")
    tqdm.write("Could not find the latest month folder. Please check the URL or connection.")
