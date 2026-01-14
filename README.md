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
│   └── config.yaml              # Arquivo de configuração para caminhos, formatos e tipos de dados
├── data_incoming                # Pasta para arquivos ZIP de dados recebidos
├── data_outgoing                # Pasta para os dados processados de saída
├── docker
│   └── mongo-init               # Scripts de inicialização do MongoDB
│       ├── 01-create-user.js    # Cria usuário do banco
│       └── create-indexes.js    # Cria índices para otimização
├── logs                         # Pasta para arquivos de log
├── scripts
│   ├── cnpj_extractor.cjs       # Script para extração de dados (parte 1)
│   ├── cnpj_merger.py           # Script para unificação das tabelas particionadas (parte 2)
│   ├── cnpj_to_jsonl.py         # Script para conversão para JSONL (parte 3)
│   ├── import_to_mongo.ps1      # Script Windows para importar no MongoDB
│   └── import_to_mongo.sh       # Script Linux/Mac para importar no MongoDB
├── docker-compose.yml           # MongoDB + Mongo Express
├── README.md                    # Documentação do projeto
└── execute_model.bat            # Exemplo de script batch para executar o projeto completo
```

## Iniciando o Projeto

### Pré-requisitos

- Node.js 18+

### Clone o repositório e instale as dependências Node.js

```bash
git clone https://github.com/jmfeck/cnpj-data-extractor.git
cd cnpj-data-extractor
npm install
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

Para iniciar o processo de extração, execute o script `cnpj_extractor.js`.

Esse script irá:

1. Acessar a URL base definida no `config.yaml`
2. Identificar a pasta mais recente com base no padrão `AAAA-MM`
3. Listar todos os arquivos `.zip` disponíveis nessa pasta
4. Verificar se cada arquivo já foi baixado anteriormente (com base no tamanho)
5. Fazer o download apenas dos arquivos necessários, utilizando múltiplos threads para acelerar o processo
6. Salvar todos os arquivos na pasta `data_incoming/`

Execute com:

```bash
npm run extract
```

## Parte 2: Unificação de Dados

Após o download dos arquivos, execute `cnpj_merger.js` para realizar o processamento dos dados.

Esse script irá:

1. Localizar todos os arquivos `.zip` na pasta `data_incoming/`
2. Identificar o tipo de cada arquivo com base no prefixo (por exemplo, `empresa`, `estabelecimento`, etc.)
3. Extrair o conteúdo de cada `.zip` (espera-se que contenha apenas um `.csv`)
4. Ler os dados aplicando os tipos definidos no `config.yaml`
5. Unificar os dados de cada tipo em um único arquivo
6. Exportar os dados consolidados para a pasta `data_outgoing/`, no formato especificado (`csv` ou `parquet`)

Execute o script com:

```bash
npm run merge
```

## Formatos Suportados

Atualmente, os formatos de exportação disponíveis são:

- `csv`
- `parquet`

## Parte 3: Exportar para MongoDB (Opcional)

Para quem deseja fazer queries mais avançadas nos dados, é possível exportar para MongoDB.

### 3.1 Gerar arquivo JSONL

O script `cnpj_to_jsonl.py` converte os CSVs em um arquivo JSONL otimizado para MongoDB, onde cada empresa é um documento completo contendo:

- Dados cadastrais da empresa
- Todos os estabelecimentos (matriz e filiais)
- Todos os sócios
- Informações do Simples Nacional/MEI

```bash
python scripts/cnpj_to_jsonl.py
```

O arquivo `empresas.jsonl` será gerado em `data_outgoing/`.

### 3.2 Subir MongoDB com Docker

O projeto inclui um `docker-compose.yml` com MongoDB e Mongo Express (interface web):

```bash
docker-compose up -d
```

Serviços disponíveis:
- **MongoDB**: `localhost:27017`
- **Mongo Express (UI)**: http://localhost:8081

### 3.3 Importar dados no MongoDB

Execute o script de importação que também cria os índices otimizados:

```powershell
# Windows PowerShell
.\scripts\import_to_mongo.ps1
```

```bash
# Linux/Mac
./scripts/import_to_mongo.sh
```

### 3.4 Conexão com MongoDB

| Tipo | String de conexão |
|------|-------------------|
| Aplicação | `mongodb://cnpj_user:cnpj123@localhost:27017/cnpj` |
| Admin | `mongodb://admin:admin123@localhost:27017` |

### 3.5 Estrutura do documento

Cada documento representa uma empresa completa:

```json
{
  "_id": "12345678",
  "cnpj_basico": "12345678",
  "razao_social": "EMPRESA EXEMPLO LTDA",
  "natureza_juridica": { "codigo": "2062", "descricao": "Sociedade Empresária Limitada" },
  "capital_social": 100000.0,
  "porte": { "codigo": "01", "descricao": "Micro Empresa" },
  "estabelecimentos": [
    {
      "cnpj": "12345678000190",
      "cnpj_formatado": "12.345.678/0001-90",
      "matriz": true,
      "nome_fantasia": "EXEMPLO",
      "situacao_cadastral": { "codigo": "02", "descricao": "Ativa" },
      "endereco": { "uf": "SP", "municipio": { "codigo": "7107", "nome": "SAO PAULO" } },
      "contato": { "email": "contato@exemplo.com" },
      "cnae_principal": { "codigo": "6201501", "descricao": "..." }
    }
  ],
  "socios": [
    { "nome": "FULANO DA SILVA", "tipo": { "codigo": "2", "descricao": "Pessoa Física" } }
  ],
  "simples": { "optante_simples": true, "optante_mei": false }
}
```

### 3.6 Índices criados

Os seguintes índices são criados automaticamente para otimizar queries:

- `razao_social` (text search)
- `estabelecimentos.endereco.uf`
- `estabelecimentos.endereco.municipio.codigo`
- `estabelecimentos.cnae_principal.codigo`
- `estabelecimentos.situacao_cadastral.codigo`
- `estabelecimentos.cnpj`
- `socios.nome`
- Índice composto: `uf + situacao_cadastral`

### 3.7 Exemplos de queries

```javascript
// Buscar por razão social (text search)
db.empresas.find({ $text: { $search: "restaurante" } })

// Empresas ativas em São Paulo
db.empresas.find({
  "estabelecimentos.endereco.uf": "SP",
  "estabelecimentos.situacao_cadastral.codigo": "02"
})

// Buscar por CNPJ completo
db.empresas.find({ "estabelecimentos.cnpj": "12345678000190" })

// Empresas por CNAE
db.empresas.find({ "estabelecimentos.cnae_principal.codigo": "6201501" })

// Buscar sócio por nome
db.empresas.find({ "socios.nome": /SILVA/ })

// Optantes do Simples Nacional
db.empresas.find({ "simples.optante_simples": true })
```

## Logs

Os arquivos de log são gerados automaticamente na pasta `logs/`, permitindo acompanhar erros, tempo de execução e progresso geral.

## Contribuindo

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests.

## Licença

Este projeto está licenciado sob a Licença MIT.
