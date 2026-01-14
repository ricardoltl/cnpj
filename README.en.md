# CNPJ Data Extractor

> 📘 **Versão em português disponível aqui** → [README.md](README.md)

## Project Overview

The CNPJ Data Extractor is an open-source project that automates the download, extraction, and transformation of CNPJ (Brazilian company registry) datasets from public sources. The project is divided into two parts:

1. **Data Extraction**: Automatically download and extract partitioned CNPJ datasets.
2. **Data Merging**: Combine the partitioned tables into consolidated datasets for further processing or analysis.

## Features

- **Automated Data Download**: Multithreaded download of datasets with remote size check to avoid redundant downloads.
- **Efficient Data Processing**: Handles large partitioned datasets and consolidates them into a unified output.
- **Flexible Export Formats**: Supports CSV and Parquet.
- **Modular Configuration**: Paths, logs, and export options are easily configurable via a `config.yaml` file.

## Project Structure

```
.  
├── config  
│   └── config.yaml         # Configuration file for paths, formats, and data types  
├── data_incoming           # Folder for incoming ZIP data files  
├── data_outgoing           # Folder for processed output data  
├── logs                    # Folder for log files  
├── scripts                 # Node.js scripts  
│   ├── cnpj_extractor.js   # Script for data extraction (part 1)  
│   └── cnpj_merger.js      # Script for merging partitioned tables (part 2)
├── README.md               # Project documentation  
└── execute_model.bat       # Batch script example for executing the full process (adjust your environment)
```

## Getting Started

### Requirements

- Node.js 18+

### Clone the repository and install Node.js dependencies

```bash
git clone https://github.com/jmfeck/cnpj-data-extractor.git
cd cnpj-data-extractor
npm install
```

### Configuration

Before running the scripts, make sure the `config.yaml` file is properly configured. It contains the base URL, CSV reading parameters, export format, and expected data types for each table.

**Example `config.yaml`**:

```yaml
# Base URL for the CNPJ dataset
base_url: 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj'

# CSV settings
csv_sep: ';'
csv_dec: ','
csv_quote: '"'
csv_enc: 'latin1'

# Export format: 'csv' or 'parquet'
export_format: 'parquet'

# Data types definition for the "empresa" table
dtypes:
  empresa:
    cnpj_basico: "str"
    razao_social: "str"
    natureza_juridica: "str"
    qualificacao_responsavel: "str"
    capital_social: "float"
    porte_empresa: "str"
    ente_federativo_responsavel: "str"
```

## Part 1: Data Extraction

To start the process, run the `cnpj_extractor.js` script.

This script will:

1. Access the base URL defined in `config.yaml`
2. Identify the latest folder using the `YYYY-MM` pattern
3. List all `.zip` files available in the folder
4. Check if each file has already been downloaded (using file size)
5. Download only the necessary files using multithreading
6. Save all files to the `data_incoming/` folder

Run with:

```bash
npm run extract
```

## Part 2: Data Merging

After downloading the files, run `cnpj_merger.js` to process the data.

This script will:

1. Read all `.zip` files from the `data_incoming/` folder
2. Detect the type of each file based on its prefix (e.g., `empresa`, `estabelecimento`, etc.)
3. Extract the `.csv` from each `.zip` (expects only one CSV per archive)
4. Apply data types as defined in `config.yaml`
5. Merge the data of each type into a single consolidated file
6. Export the result to the `data_outgoing/` folder in the configured format (`csv` or `parquet`)

Run with:

```bash
npm run merge
```

## Supported Formats

Currently supported export formats:

- `csv`
- `parquet`

Support for other formats like JSON or Feather may be added in the future.

## Logs

Log files are automatically saved in the `logs/` folder, allowing you to monitor errors, progress, and execution time.

## Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request.

## License

This project is licensed under the MIT License.
