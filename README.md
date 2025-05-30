# CNPJ Data Extractor

## Video Tutorial

Para uma apresentacao em video deste projeto, acesse: [CNPJ Data Extractor - Video Tutorial](https://www.youtube.com/watch?v=PQhjDoVe2vg)

## Visão Geral do Projeto
O CNPJ Data Extractor é um projeto de código aberto que automatiza o processo de download, extração e transformação de conjuntos de dados do CNPJ (Cadastro Nacional da Pessoa Jurídica) a partir de fontes públicas disponíveis. O projeto é dividido em duas partes:

1. **Extração de Dados**: Baixar e extrair automaticamente os conjuntos de dados do CNPJ particionados.
2. **Unificação de Dados**: Combinar as tabelas particionadas em conjuntos de dados consolidados para processamento ou análise posterior.

## Funcionalidades

- **Download Automático de Dados**: Download multithreaded dos conjuntos de dados com suporte para verificação remota do tamanho do arquivo, evitando downloads redundantes.
- **Processamento Eficiente de Dados**: Lida com grandes conjuntos de dados particionados e os consolida em uma única saída.
- **Formatos de Exportação Flexíveis**: Os dados podem ser exportados nos formatos CSV, Parquet, JSON ou Feather para fácil integração com bancos de dados e plataformas de análise.
- **Configuração Modular**: Caminhos, logs e opções de exportação são facilmente ajustáveis por meio de um arquivo de configuração (`config.yaml`).

## Estrutura do Projeto

```
.  
├── config  
  ├── config.yaml         # Arquivo de configuração para caminhos, formatos e tipos de dados  
├── data_incoming         # Pasta para arquivos ZIP de dados recebidos  
├── data_outgoing         # Pasta para os dados processados de saída  
├── logs                  # Pasta para arquivos de log  
├── scripts               # Pasta para scripts em Python  
  ├── cnpj_extractor.py   # Script para extração de dados (parte 1)  
  ├── cnpj_merger.py      # Script para unificação das tabelas particionadas (parte 2)  
├── README.md             # Documentação do projeto  
```

## Iniciando o Projeto

### Pré-requisitos

- Python 3.8+
- Os seguintes pacotes Python:
  - pandas
  - pyyaml
  - requests
  - bs4 (BeautifulSoup)
  - tqdm

### Configuração

Antes de executar os scripts, certifique-se de que o arquivo `config.yaml` esteja configurado de acordo com o seu ambiente. Esse arquivo contém caminhos para armazenamento de dados, logs e configurações de formato para exportação de dados.

**Exemplo de config.yaml**:

```yaml
# URL base para o conjunto de dados do CNPJ  
base_url: 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj'  

# Configurações de CSV  
csv_sep: ';'  
csv_dec: ','  
csv_quote: '"'  
csv_enc: 'cp1252'  

# Formato de exportação: Escolha entre 'csv', 'parquet', 'json' ou 'feather'  
export_format: 'parquet'  

# Definições de tipo de dado para cada tabela  
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

## Parte 1: Extração de Dados

Para iniciar o processo de extração de dados, execute o script `cnpj_extractor.py`. Este script fará o download do conjunto de dados do CNPJ para o mês mais recente disponível, verificará o tamanho dos arquivos e evitará o re-download de arquivos que já existam.

Execute a extração:

```bash
python cnpj_extractor.py  
```

Isso irá:

- Baixar os conjuntos de dados particionados do mês mais recente disponível no servidor.
- Salvá-los na pasta `data_incoming`.

## Parte 2: Unificação de Dados

Após a extração dos dados, a segunda parte envolve a unificação das tabelas particionadas em conjuntos de dados consolidados.

Para realizar a unificação, execute o script `cnpj_merger.py`. Esse script lê os arquivos de dados, os processa e salva os dados unificados na pasta `data_outgoing` no formato especificado no `config.yaml`.

Execute o processo de unificação:

```bash
python cnpj_merger.py  
```

Isso irá:

- Unir os arquivos de dados particionados (por exemplo, Empresas, Estabelecimentos, Sócios) em um único conjunto de dados.
- Salvar o conjunto de dados unificado na pasta `data_outgoing` no formato que você preferir.

## Personalizando o Formato de Exportação

Você pode facilmente alterar o formato dos arquivos exportados (por exemplo, CSV, Parquet, JSON) modificando o valor de `export_format` no `config.yaml`. Os formatos suportados são:

- csv
- parquet
- json
- feather

## Logs

Os logs são gerados na pasta `logs`, com detalhes sobre os processos de download, extração e unificação. Isso ajuda a acompanhar o progresso das atividades de extração e unificação de dados.

## Contribuindo

Contribuições são bem-vindas! Se você encontrar bugs ou tiver sugestões para novos recursos ou melhorias, sinta-se à vontade para enviar uma issue ou abrir um pull request.

## Licença

Este projeto é licenciado sob a Licença MIT.

## Notas para Melhorias

- **Tratamento de Erros**: Melhorias adicionais podem ser incluídas, como mecanismos de tentativa de novo download ou tratamento de arquivos parcialmente baixados.

