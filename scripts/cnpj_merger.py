import pandas as pd
import logging
import os
import zipfile
import yaml

########################## Load Configurations ##########################
data_incoming_foldername = 'data_incoming'
data_outgoing_foldername = 'data_outgoing'
log_foldername = 'logs'
log_filename = 'cnpj_merger.log'
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
os.makedirs(path_outgoing, exist_ok=True)

with open(path_config, 'r') as file:
    config = yaml.safe_load(file)

csv_sep = config['csv_sep']
csv_dec = config['csv_dec']
csv_quote = config['csv_quote']
csv_enc = config['csv_enc']
export_format = config['export_format']

# Data types for each table
dtypes = config['dtypes']

########################## Functions ##########################

def process_and_merge_files(file_params, dtype_dict, prefix, filter_condition=None):
    """Read and merge files from ZIP archives based on specific conditions."""
    dfs = []  # List to store DataFrames
    columns = list(dtype_dict.keys())  # Get column names from dtype_dict keys

    for file_list in file_params:
        zip_file_path = file_list[0]
        zip_filename = file_list[1]
        logging.info(f'Reading from ZIP: {zip_filename}')
        print(f'Reading from ZIP: {zip_filename}')

        try:
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_file_list = zip_ref.namelist()
                if len(zip_file_list) == 1:
                    with zip_ref.open(zip_file_list[0]) as csvfile:
                        df_buff = pd.read_csv(csvfile, sep=csv_sep, decimal=csv_dec, quotechar=csv_quote, dtype=str, encoding=csv_enc, header=None, keep_default_na=False)
                        df_buff.columns = columns
                        for column, dtype in dtype_dict.items():
                            if dtype == bool:
                                df_buff[column] = df_buff[column].astype(int).astype(bool) # file conversion safety in case of bolean
                            else:
                                df_buff[column] = df_buff[column].astype(dtype)

                    if filter_condition is not None:
                        df_buff = filter_condition(df_buff)

                    # print(df_buff.dtypes)
                    logging.info(f'Appending: {zip_filename}')
                    print(f'Appending: {zip_filename}')
                    dfs.append(df_buff)

                else:
                    logging.warning(f'ZIP file {zip_filename} contains more than one file.')
                    print(f'Warning: ZIP file {zip_filename} contains more than one file.')

        except Exception as e:
            logging.error(f"Error processing {zip_filename}: {e}")
            print(f"Error processing {zip_filename}: {e}")
    
    if dfs:
        merged_df = pd.concat(dfs, ignore_index=True)
        merged_df.columns = columns
    else:
        merged_df = pd.DataFrame(columns=dtype_dict.keys())

    return merged_df

def export_dataframe(df, export_path):
    """Exports the DataFrame to the specified format."""
    export_format = export_path.split('.')[-1]
    if export_format == "csv":
        df.to_csv(export_path, index=False, sep=csv_sep, encoding=csv_enc, quotechar=csv_quote)
    elif export_format == "parquet":
        df.to_parquet(export_path, index=False)
    elif export_format == "json":
        df.to_json(export_path, orient="records", lines=True)
    elif export_format == "feather":
        df.to_feather(export_path)
    else:
        raise ValueError(f"Unsupported export format: {export_format}")

########################## Main ##########################

logging.basicConfig(filename=path_log, level=logging.INFO, format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
logging.info('Starting script')
print('Starting script')

### Mapping incoming files
logging.info('Mapping incoming files')
print('Mapping incoming files')

# Parameters for each table
file_params = {prefix: [] for prefix in dtypes.keys()}

# Crawling through directory and subdirectories to find ZIP files
for root, directories, files in os.walk(path_incoming): 
    for filename in files: 
        if filename.endswith(".zip"):
            file_with_no_ext = filename.split('.')[0]
            zip_file_path = os.path.join(root, filename)
            for prefix in file_params:
                if file_with_no_ext.startswith(prefix.title()):
                    file_params[prefix].append([zip_file_path, filename, file_with_no_ext])

# Processing and exporting files for all tables
for prefix, params in file_params.items():
    dtypes_var = dtypes[prefix]  # Get dtypes for the prefix
    df_merged = process_and_merge_files(params, dtypes_var, prefix)
    logging.info(f'Exporting: {prefix}')
    print(f'Exporting: {prefix}')
    outgoing_file_path = os.path.join(path_outgoing, prefix + '.' + export_format)
    export_dataframe(df_merged, outgoing_file_path)