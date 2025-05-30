# CNPJ Data Extractor

> 📘 **English version available here** → [README.en.md](README.en.md)

## Video Tutorial

Para uma apresentação em vídeo deste projeto, acesse: [CNPJ Data Extractor - Video Tutorial](https://www.youtube.com/watch?v=PQhjDoVe2vg)

## Visão Geral do Projeto

O CNPJ Data Extractor é um projeto de código aberto que automatiza o processo de download, extração e transformação de conjuntos de dados do CNPJ (Cadastro Nacional da Pessoa Jurídica) a partir de fontes públicas disponíveis. O projeto é dividido em duas partes:

1. **Extração de Dados**: Baixar e extrair automaticamente os conjuntos de dados do CNPJ particionados.
2. **Unificação de Dados**: Combinar as tabelas particionadas em conjuntos de dados consolidados para processamento ou análise posterior.

## Funcionalidades

- **Download Automático de Dados**: Download multithreaded dos conjuntos de dados com verificação de tamanho dos arquivos remotos, evitando downloads redundantes.
- **Processamento Eficiente de Dados**: Lida com grandes volumes de dados particionados e os consolida em uma única saída.
- **Formatos de Exportação Flexíveis**: Suporte a CSV e Parquet.
- **Configuração Modular**: Caminhos, logs e opções de exportação são facilmente ajustáveis por meio de um arquivo de configuração (`config.yaml`).

## Estrutura do Projeto

```
.  
├── config  
│   └── config.yaml         # Arquivo de configuração para caminhos, formatos e tipos de dados  
├── data_incoming           # Pasta para arquivos ZIP de dados recebidos  
├── data_outgoing           # Pasta para os dados processados de saída  
├── logs                    # Pasta para arquivos de log  
├── scripts                 # Pasta para scripts em Python  
│   ├── cnpj_extractor.py   # Script para extração de dados (parte 1)  
│   └── cnpj_merger.py      # Script para unificação das tabelas particionadas (parte 2)
├── README.md               # Documentação do projeto 
└── execute_model.bat       # Exemplo de script batch para executar o projeto completo (configure o ambiente antes)
```

## Iniciando o Projeto

### Pré-requisitos

- Python 3.12+

### Clone o repositório, crie um ambiente virtual em Python e instale as dependências

```bash
git clone https://github.com/jmfeck/cnpj-data-extractor.git
cd cnpj-data-extractor
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### Configuração

Antes de executar os scripts, certifique-se de que o arquivo `config.yaml` esteja configurado corretamente. Esse arquivo contém a URL base, parâmetros de leitura de CSV, tipo de exportação e os tipos de dados esperados para cada tabela.

**Exemplo de config.yaml**:

```yaml
# URL base para o conjunto de dados do CNPJ
base_url: 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj'

# Configurações de CSV
csv_sep: ';'
csv_dec: ','
csv_quote: '"'
csv_enc: 'latin1'

# Formato de exportação: 'csv' ou 'parquet'
export_format: 'parquet'

# Definições de tipo de dado para cada tabela
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

## Parte 1: Extração de Dados

Para iniciar o processo de extração, execute o script `cnpj_extractor.py`.

Esse script irá:

1. Acessar a URL base definida no `config.yaml`
2. Identificar a pasta mais recente com base no padrão `AAAA-MM`
3. Listar todos os arquivos `.zip` disponíveis nessa pasta
4. Verificar se cada arquivo já foi baixado anteriormente (com base no tamanho)
5. Fazer o download apenas dos arquivos necessários, utilizando múltiplos threads para acelerar o processo
6. Salvar todos os arquivos na pasta `data_incoming/`

Execute com:

```bash
python cnpj_extractor.py
```

## Parte 2: Unificação de Dados

Após o download dos arquivos, execute `cnpj_merger.py` para realizar o processamento dos dados.

Esse script irá:

1. Localizar todos os arquivos `.zip` na pasta `data_incoming/`
2. Identificar o tipo de cada arquivo com base no prefixo (por exemplo, `empresa`, `estabelecimento`, etc.)
3. Extrair o conteúdo de cada `.zip` (espera-se que contenha apenas um `.csv`)
4. Ler os dados aplicando os tipos definidos no `config.yaml`
5. Unificar os dados de cada tipo em um único arquivo
6. Exportar os dados consolidados para a pasta `data_outgoing/`, no formato especificado (`csv` ou `parquet`)

Execute o script com:

```bash
python cnpj_merger.py
```

## Formatos Suportados

Atualmente, os formatos de exportação disponíveis são:

- `csv`
- `parquet`

Outros formatos como JSON ou Feather podem ser adicionados no futuro.

## Logs

Os arquivos de log são gerados automaticamente na pasta `logs/`, permitindo acompanhar erros, tempo de execução e progresso geral.

## Contribuindo

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests.

## Licença

Este projeto está licenciado sob a Licença MIT.