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
import sys
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
config_foldername = 'config'
config_filename = 'config.yaml'

path_script = os.path.abspath(__file__)
path_script_dir = os.path.dirname(path_script)
path_project = os.path.dirname(path_script_dir)
path_incoming = os.path.join(path_project, data_incoming_foldername)
path_outgoing = os.path.join(path_project, data_outgoing_foldername)
path_config_dir = os.path.join(path_project, config_foldername)
path_config = os.path.join(path_config_dir, config_filename)

# Ensure incoming folder exists
os.makedirs(path_incoming, exist_ok=True)

with open(path_config, 'r') as file:
    config = yaml.safe_load(file)

# Root URL to fetch folders
base_url = config['base_url']

# Detect if running in interactive terminal (TTY)
IS_INTERACTIVE = sys.stdout.isatty()

# Set up logging (apenas console, sem arquivo)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def log_message(message):
    """Helper function to log messages appropriately based on environment."""
    logging.info(message)
    if not IS_INTERACTIVE:
        # In Docker/non-interactive, messages are already logged via console_handler
        pass
    else:
        # In interactive mode with tqdm, use tqdm.write to avoid interfering with progress bars
        tqdm.write(message)


def get_remote_file_size(url):
    """Fetch the file size of the remote file from HTTP headers."""
    try:
        response = requests.head(url)
        response.raise_for_status()
        return int(response.headers.get('content-length', 0))  # Return the remote file size
    except requests.RequestException as e:
        logging.error(f"Failed to fetch file size for {url}: {e}")
        if IS_INTERACTIVE:
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
        error_msg = f"Failed to fetch website directories: {e}"
        logging.error(error_msg)
        if IS_INTERACTIVE:
            tqdm.write(error_msg)
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
        error_msg = f"Failed to fetch files from folder: {e}"
        logging.error(error_msg)
        if IS_INTERACTIVE:
            tqdm.write(error_msg)
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
            log_message(f"File {local_file_name} already exists and matches the size. Skipping download.")
            return local_file_path
        else:
            log_message(f"File {local_file_name} exists but size does not match. Re-downloading.")
    
    # If the file doesn't exist or the sizes don't match, download the file
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))  # Total file size in bytes
            chunk_size = 8192  # Define chunk size (8 KB)
            
            # For non-interactive environments (Docker), log progress periodically
            if not IS_INTERACTIVE:
                log_message(f"Starting download: {local_file_name} ({total_size / (1024*1024):.2f} MB)")
                downloaded = 0
                last_log_percent = 0
                
                with open(local_file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Log progress every 10%
                        if total_size > 0:
                            percent = int((downloaded / total_size) * 100)
                            if percent >= last_log_percent + 10:
                                log_message(f"  {local_file_name}: {percent}% ({downloaded / (1024*1024):.2f} MB / {total_size / (1024*1024):.2f} MB)")
                                last_log_percent = percent
            else:
                # Interactive mode with tqdm progress bar
                with open(local_file_path, 'wb') as f:
                    with tqdm(total=total_size, unit='B', unit_scale=True, desc=local_file_name, ncols=80) as pbar:
                        for chunk in r.iter_content(chunk_size=chunk_size): 
                            f.write(chunk)
                            pbar.update(len(chunk))
            
        log_message(f"Downloaded {local_file_name} successfully.")
        return local_file_path
    except requests.exceptions.RequestException as e:
        error_msg = f"Failed to download {local_file_name}: {e}"
        logging.error(error_msg)
        if IS_INTERACTIVE:
            tqdm.write(error_msg)
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
    log_message(f"Latest month folder: {latest_folder}")
    # Build the full folder URL for the latest month
    folder_url = base_url + '/' + latest_folder + '/'
    
    # Get all files in the folder
    files_in_folder = get_all_files_in_folder(folder_url)
    
    if files_in_folder:
        log_message(f"Found {len(files_in_folder)} files in folder {latest_folder}")
        
        # Create full download URLs for each file
        list_of_urls = [folder_url + file for file in files_in_folder]
        
        # Get available threads/CPU cores
        available_threads = get_available_threads()
        log_message(f"Number of available threads (CPU cores): {available_threads}")
        
        # Set up the ThreadPoolExecutor to download files in parallel
        with ThreadPoolExecutor(max_workers=available_threads) as executor:
            # Submit the download tasks to the executor
            futures = {executor.submit(download_file_parallel, url): url for url in list_of_urls}
            
            # Process each future as it completes
            for future in as_completed(futures):
                url = futures[future]
                try:
                    result = future.result()
                    # Success message is already logged inside download_file
                except Exception as e:
                    error_msg = f"Error downloading {url}: {e}"
                    logging.error(error_msg)
                    if IS_INTERACTIVE:
                        tqdm.write(error_msg)

        # Optional: Print a summary of successful and failed downloads
        log_message(f"Finished downloading files.")
    else:
        log_message(f"No files found in the folder: {latest_folder}")
else:
    log_message("Could not find the latest month folder. Please check the URL or connection.")
