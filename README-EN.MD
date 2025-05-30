# CNPJ Data Extractor

## Project Overview
The CNPJ Data Extractor is an open-source project that automates the process of downloading, extracting, and transforming CNPJ (Cadastro Nacional da Pessoa Jurídica) datasets from publicly available sources. The project is divided into two parts:

1. **Data Extraction**: Automatically download and extract partitioned CNPJ datasets.
2. **Data Unification**: Combine the partitioned tables into consolidated datasets for further processing or analysis.

## Features

- **Automatic Data Download**: Multithreaded download of datasets with support for remote file size verification, avoiding redundant downloads.
- **Efficient Data Processing**: Handles large partitioned datasets and consolidates them into a single output.
- **Flexible Export Formats**: Data can be exported in CSV, Parquet, JSON, or Feather formats for easy integration with databases and analysis platforms.
- **Modular Configuration**: Paths, logs, and export options are easily adjustable via a configuration file (`config.yaml`).

## Project Structure

```
.  
├── config  
  ├── config.yaml         # Configuration file for paths, formats, and data types  
├── data_incoming         # Folder for received data ZIP files  
├── data_outgoing         # Folder for processed output data  
├── logs                  # Folder for log files  
├── scripts               # Folder for Python scripts  
  ├── cnpj_extractor.py   # Script for data extraction (part 1)  
  ├── cnpj_merger.py      # Script for unifying partitioned tables (part 2)  
├── README.md             # Project documentation  
```

## Getting Started

### Prerequisites

- Python 3.8+
- The following Python packages:
  - pandas
  - pyyaml
  - requests
  - bs4 (BeautifulSoup)
  - tqdm

### Configuration

Before running the scripts, make sure the `config.yaml` file is configured according to your environment. This file contains paths for data storage, logs, and format settings for data export.

**Example config.yaml**:

```yaml
# Base URL for the CNPJ dataset  
base_url: 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj'  

# CSV settings  
csv_sep: ';'  
csv_dec: ','  
csv_quote: '"'  
csv_enc: 'latin1'  

# Export format: Choose between 'csv', 'parquet', 'json', or 'feather'  
export_format: 'parquet'  

# Data type definitions for each table  
dtypes:  
  empresas:  
    cnpj_basico: "str"  
    razao_social: "str"  
    natureza_juridica: "str"  
    qualificacao_do_responsavel: "str"  
    capital_social: "str"  
    porte_da_empresa: "str"  
    ente_federativo_resposavel: "str"  

  estabelecimentos:  
    cnpj_basico: "str"  
    cnpj_ordem: "str"  
    cnpj_dv: "str"  
    identificador_matriz_filial: "str"  
    nome_fantasia: "str"  
    situacao_cadastral: "str"  
    data_situacao_cadastral: "str"  
    motivo_situacao_cadastral: "str"  
    nome_da_cidade_no_exterior: "str"  
    pais: "str"  
    data_de_inicio_da_atividade: "str"  
    cnae_fiscal_principal: "str"  
    cnae_fiscal_secundaria: "str"  
    tipo_de_logradouro: "str"  
    logradouro: "str"  
    numero: "str"  
    complemento: "str"  
    bairro: "str"  
    cep: "str"  
    uf: "str"  
    municipio: "str"  
    situacao_cadastral: "str"  
    ddd1: "str"  
    telefone1: "str"  
    ddd2: "str"  
    telefone2: "str"  
    ddd_do_fax: "str"  
    fax: "str"  
    correio_eletronico: "str"  
    situacao_especial: "str"  
    data_da_situacao_especial: "str"  
  ...  
```

## Part 1: Data Extraction

To start the data extraction process, run the `cnpj_extractor.py` script. This script will download the CNPJ dataset for the most recent month available, verify the file sizes, and avoid re-downloading files that already exist.

Run the extraction:

```bash
python cnpj_extractor.py  
```

This will:

- Download the partitioned datasets for the most recent month available on the server.
- Save them in the `data_incoming` folder.

## Part 2: Data Unification

After data extraction, the second part involves unifying the partitioned tables into consolidated datasets.

To perform the unification, run the `cnpj_merger.py` script. This script reads the data files, processes them, and saves the unified data in the `data_outgoing` folder in the format specified in `config.yaml`.

Run the unification process:

```bash
python cnpj_merger.py  
```

This will:

- Merge the partitioned data files (e.g., Companies, Establishments, Partners) into a single dataset.
- Save the unified dataset in the `data_outgoing` folder in your preferred format.

## Customizing the Export Format

You can easily change the format of the exported files (e.g., CSV, Parquet, JSON) by modifying the `export_format` value in `config.yaml`. The supported formats are:

- csv
- parquet
- json
- feather

## Logs

Logs are generated in the `logs` folder, providing details about the download, extraction, and unification processes. This helps track the progress of data extraction and unification activities.

## Contributing

Contributions are welcome! If you find any bugs or have suggestions for new features or improvements, feel free to submit an issue or open a pull request.

## License

This project is licensed under the MIT License.

## Improvement Notes

- **Error Handling**: Additional improvements can be included, such as retry mechanisms for downloads or handling partially downloaded files.
