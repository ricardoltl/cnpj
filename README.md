# CNPJ Data Extractor

Este projeto extrai e processa informações de CNPJ (Cadastro Nacional da Pessoa Jurídica) de empresas brasileiras a partir de datasets públicos disponibilizados pela Receita Federal.

**Criado por:** João M. Feck  
**GitHub:** [jmfeck](https://github.com/jmfeck)  
**Email:** joaomfeck@gmail.com

## 🚀 Instalação e Configuração

Este projeto usa [uv](https://github.com/astral-sh/uv) para gerenciamento de dependências, que é extremamente rápido e simples de usar.

### Pré-requisitos

- Python 3.12 ou superior
- uv instalado (você já tem instalado via Homebrew)

### Quick Start

```bash
# 1. Clone o repositório (se ainda não fez)
cd /caminho/para/o/projeto

# 2. Instale as dependências
uv sync

# 3. Execute qualquer script
uv run python scripts/cnpj_extractor.py
```

### Instalação das Dependências

```bash
# No diretório do projeto, execute:
uv sync
```

Este comando irá:
- Criar um ambiente virtual automaticamente (`.venv`)
- Instalar todas as dependências especificadas no `pyproject.toml`
- Configurar o projeto para execução

## 📦 Uso

### Executar os scripts

#### Opção 1: Usando uv run (recomendado)

```bash
# Extrator de CNPJ
uv run python scripts/cnpj_extractor.py

# Mesclador de CNPJ
uv run python scripts/cnpj_merger.py

# Importador para PostgreSQL
uv run python scripts/import_to_postgres.py
```

#### Opção 2: Ativando o ambiente virtual

```bash
# Ativar o ambiente virtual
source .venv/bin/activate  # No macOS/Linux
# ou
.venv\Scripts\activate  # No Windows

# Executar os scripts
python scripts/cnpj_extractor.py
python scripts/cnpj_merger.py
python scripts/import_to_postgres.py

# Desativar quando terminar
deactivate
```

## 📁 Estrutura do Projeto

```
cnpj/
├── config/
│   └── config.yaml          # Configurações do projeto
├── scripts/
│   ├── cnpj_extractor.py    # Script de extração de dados
│   ├── cnpj_merger.py       # Script de mesclagem de dados
│   ├── import_to_postgres.py # Script de importação para PostgreSQL
│   └── import_to_mongodb.py # Script de importação para MongoDB
├── docker-compose.yml       # Docker Compose para PostgreSQL + pgAdmin
├── pyproject.toml           # Configuração do projeto e dependências
├── .python-version          # Versão do Python
├── POSTGRES.md             # Guia detalhado do PostgreSQL
└── README.md               # Este arquivo
```

## 🐘 PostgreSQL

O projeto inclui um `docker-compose.yml` para subir uma instância do PostgreSQL + pgAdmin:

```bash
# Subir o PostgreSQL
docker compose up -d

# Importar dados dos arquivos .parquet
uv run python scripts/import_to_postgres.py
```

Consulte o [POSTGRES.md](POSTGRES.md) para instruções detalhadas e exemplos de consultas.

## ⚙️ Configuração

As configurações do projeto estão no arquivo `config/config.yaml`. Você pode ajustar:
- URL base dos arquivos da Receita Federal
- Configurações de CSV (separador, codificação, etc.)
- Formato de exportação (parquet, csv, json, feather)
- Definições de tipos de dados para cada tabela

## 🔧 Comandos Úteis do uv

```bash
# Adicionar uma nova dependência
uv add nome-do-pacote

# Remover uma dependência
uv remove nome-do-pacote

# Atualizar dependências
uv sync --upgrade

# Limpar o ambiente
rm -rf .venv
uv sync
```

## 📝 Notas

- O projeto usa Python 3.12.7
- As dependências são gerenciadas no `pyproject.toml`
- O uv cria automaticamente um ambiente virtual em `.venv`
