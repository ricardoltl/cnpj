import pandas as pd
import json
import logging
import os
import yaml
from tqdm import tqdm

########################## Load Configurations ##########################
data_outgoing_foldername = 'data_outgoing'
log_foldername = 'logs'
log_filename = 'cnpj_to_jsonl.log'
config_foldername = 'config'
config_filename = 'config.yaml'

path_script = os.path.abspath(__file__)
path_script_dir = os.path.dirname(path_script)
path_project = os.path.dirname(path_script_dir)
path_outgoing = os.path.join(path_project, data_outgoing_foldername)
path_log_dir = os.path.join(path_project, log_foldername)
path_log = os.path.join(path_log_dir, log_filename)
path_config_dir = os.path.join(path_project, config_foldername)
path_config = os.path.join(path_config_dir, config_filename)

os.makedirs(path_log_dir, exist_ok=True)

with open(path_config, 'r') as file:
    config = yaml.safe_load(file)

csv_sep = config['csv_sep']
csv_enc = config['csv_enc']

########################## Logging Setup ##########################
logging.basicConfig(
    filename=path_log,
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)

def log(message):
    logging.info(message)
    tqdm.write(message)

########################## Load Lookup Tables ##########################
def load_lookup_tables():
    """Carrega as tabelas de lookup (códigos -> descrições)"""
    log("Carregando tabelas de lookup...")

    lookups = {}

    # CNAEs
    cnaes_path = os.path.join(path_outgoing, 'cnaes.csv')
    if os.path.exists(cnaes_path):
        df = pd.read_csv(cnaes_path, sep=csv_sep, encoding=csv_enc, dtype=str)
        lookups['cnaes'] = dict(zip(df['cnpj_basico'], df['cnae_fiscal_secundaria']))
        log(f"  CNAEs: {len(lookups['cnaes'])} registros")

    # Municípios
    municipios_path = os.path.join(path_outgoing, 'municipios.csv')
    if os.path.exists(municipios_path):
        df = pd.read_csv(municipios_path, sep=csv_sep, encoding=csv_enc, dtype=str)
        lookups['municipios'] = dict(zip(df['codigo_municipio'], df['nome_municipio']))
        log(f"  Municípios: {len(lookups['municipios'])} registros")

    # Natureza Jurídica
    natureza_path = os.path.join(path_outgoing, 'natureza.csv')
    if os.path.exists(natureza_path):
        df = pd.read_csv(natureza_path, sep=csv_sep, encoding=csv_enc, dtype=str)
        lookups['natureza'] = dict(zip(df['codigo_natureza_juridica'], df['descricao_natureza_juridica']))
        log(f"  Natureza Jurídica: {len(lookups['natureza'])} registros")

    # Qualificações
    qualificacoes_path = os.path.join(path_outgoing, 'qualificacoes.csv')
    if os.path.exists(qualificacoes_path):
        df = pd.read_csv(qualificacoes_path, sep=csv_sep, encoding=csv_enc, dtype=str)
        lookups['qualificacoes'] = dict(zip(df['codigo_qualificacao'], df['descricao_qualificacao']))
        log(f"  Qualificações: {len(lookups['qualificacoes'])} registros")

    # Países
    paises_path = os.path.join(path_outgoing, 'paises.csv')
    if os.path.exists(paises_path):
        df = pd.read_csv(paises_path, sep=csv_sep, encoding=csv_enc, dtype=str)
        lookups['paises'] = dict(zip(df['codigo_pais'], df['nome_pais']))
        log(f"  Países: {len(lookups['paises'])} registros")

    # Motivos
    motivos_path = os.path.join(path_outgoing, 'motivos.csv')
    if os.path.exists(motivos_path):
        df = pd.read_csv(motivos_path, sep=csv_sep, encoding=csv_enc, dtype=str)
        lookups['motivos'] = dict(zip(df['codigo_motivo'], df['descricao_motivo']))
        log(f"  Motivos: {len(lookups['motivos'])} registros")

    return lookups

########################## Data Processing ##########################
def parse_capital_social(value):
    """Converte capital social para float"""
    if pd.isna(value) or value == '':
        return 0.0
    try:
        return float(str(value).replace('.', '').replace(',', '.'))
    except:
        return 0.0

def parse_date(value):
    """Formata data de YYYYMMDD para YYYY-MM-DD"""
    if pd.isna(value) or value == '' or value == '00000000' or value == '0':
        return None
    value = str(value).strip()
    if len(value) == 8:
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    return value

def get_porte_descricao(codigo):
    """Retorna descrição do porte da empresa"""
    portes = {
        '00': 'Não informado',
        '01': 'Micro Empresa',
        '03': 'Empresa de Pequeno Porte',
        '05': 'Demais'
    }
    return portes.get(str(codigo).zfill(2), 'Não informado')

def get_situacao_descricao(codigo):
    """Retorna descrição da situação cadastral"""
    situacoes = {
        '01': 'Nula',
        '02': 'Ativa',
        '03': 'Suspensa',
        '04': 'Inapta',
        '08': 'Baixada'
    }
    return situacoes.get(str(codigo).zfill(2), 'Não informada')

def get_identificador_socio_descricao(codigo):
    """Retorna descrição do tipo de sócio"""
    tipos = {
        '1': 'Pessoa Jurídica',
        '2': 'Pessoa Física',
        '3': 'Estrangeiro'
    }
    return tipos.get(str(codigo), 'Não informado')

def build_estabelecimento(row, lookups):
    """Constrói objeto de estabelecimento"""
    cnpj_completo = f"{row['cnpj_basico']}{row['cnpj_ordem']}{row['cnpj_dv']}"

    estab = {
        'cnpj': cnpj_completo,
        'cnpj_formatado': f"{row['cnpj_basico'][:2]}.{row['cnpj_basico'][2:5]}.{row['cnpj_basico'][5:8]}/{row['cnpj_ordem']}-{row['cnpj_dv']}",
        'matriz': row['identificador_matriz_filial'] == '1',
        'nome_fantasia': row['nome_fantasia'] if pd.notna(row['nome_fantasia']) else None,
        'situacao_cadastral': {
            'codigo': row['situacao_cadastral'],
            'descricao': get_situacao_descricao(row['situacao_cadastral'])
        },
        'data_situacao_cadastral': parse_date(row['data_situacao_cadastral']),
        'motivo_situacao_cadastral': {
            'codigo': row['motivo_situacao_cadastral'],
            'descricao': lookups['motivos'].get(str(row['motivo_situacao_cadastral']).zfill(2), None)
        } if pd.notna(row['motivo_situacao_cadastral']) else None,
        'data_inicio_atividade': parse_date(row['data_de_inicio_da_atividade']),
        'cnae_principal': {
            'codigo': row['cnae_fiscal_principal'],
            'descricao': lookups['cnaes'].get(row['cnae_fiscal_principal'], None)
        } if pd.notna(row['cnae_fiscal_principal']) else None,
        'cnaes_secundarios': row['cnae_fiscal_secundaria'].split(',') if pd.notna(row['cnae_fiscal_secundaria']) and row['cnae_fiscal_secundaria'] else [],
        'endereco': {
            'tipo_logradouro': row['tipo_de_logradouro'] if pd.notna(row['tipo_de_logradouro']) else None,
            'logradouro': row['logradouro'] if pd.notna(row['logradouro']) else None,
            'numero': row['numero'] if pd.notna(row['numero']) else None,
            'complemento': row['complemento'] if pd.notna(row['complemento']) else None,
            'bairro': row['bairro'] if pd.notna(row['bairro']) else None,
            'cep': row['cep'] if pd.notna(row['cep']) else None,
            'uf': row['uf'] if pd.notna(row['uf']) else None,
            'municipio': {
                'codigo': row['municipio'],
                'nome': lookups['municipios'].get(row['municipio'], None)
            } if pd.notna(row['municipio']) else None,
            'pais': {
                'codigo': row['pais'],
                'nome': lookups['paises'].get(row['pais'], None)
            } if pd.notna(row['pais']) and row['pais'] else None
        },
        'contato': {
            'telefone1': f"({row['ddd1']}) {row['telefone1']}" if pd.notna(row['ddd1']) and pd.notna(row['telefone1']) and row['telefone1'] else None,
            'telefone2': f"({row['ddd2']}) {row['telefone2']}" if pd.notna(row['ddd2']) and pd.notna(row['telefone2']) and row['telefone2'] else None,
            'fax': f"({row['ddd_do_fax']}) {row['fax']}" if pd.notna(row['ddd_do_fax']) and pd.notna(row['fax']) and row['fax'] else None,
            'email': row['correio_eletronico'].lower() if pd.notna(row['correio_eletronico']) and row['correio_eletronico'] else None
        }
    }

    # Remove campos None do contato
    estab['contato'] = {k: v for k, v in estab['contato'].items() if v is not None}
    if not estab['contato']:
        estab['contato'] = None

    return estab

def build_socio(row, lookups):
    """Constrói objeto de sócio"""
    socio = {
        'tipo': {
            'codigo': row['identificador_de_socio'],
            'descricao': get_identificador_socio_descricao(row['identificador_de_socio'])
        },
        'nome': row['nome_do_socio'] if pd.notna(row['nome_do_socio']) else None,
        'documento': row['cnpj_ou_cpf_do_socio'] if pd.notna(row['cnpj_ou_cpf_do_socio']) else None,
        'qualificacao': {
            'codigo': row['qualificacao_do_socio'],
            'descricao': lookups['qualificacoes'].get(str(row['qualificacao_do_socio']).zfill(2), None)
        } if pd.notna(row['qualificacao_do_socio']) else None,
        'data_entrada': parse_date(row['data_de_entrada_sociedade']),
        'pais': {
            'codigo': row['pais'],
            'nome': lookups['paises'].get(row['pais'], None)
        } if pd.notna(row['pais']) and row['pais'] else None,
        'representante_legal': {
            'documento': row['representante_legal'],
            'nome': row['nome_do_representante'],
            'qualificacao': {
                'codigo': row['qualificacao_do_representante_legal'],
                'descricao': lookups['qualificacoes'].get(str(row['qualificacao_do_representante_legal']).zfill(2), None)
            }
        } if pd.notna(row['representante_legal']) and row['representante_legal'] and row['representante_legal'] != '***000000**' else None,
        'faixa_etaria': row['faixa_etaria'] if pd.notna(row['faixa_etaria']) else None
    }

    return socio

def build_simples(row):
    """Constrói objeto do Simples Nacional"""
    return {
        'optante_simples': row['opcao_pelo_simples'] == 'S',
        'data_opcao_simples': parse_date(row['data_opcao_simples']),
        'data_exclusao_simples': parse_date(row['data_exclusao_simples']),
        'optante_mei': row['opcao_pelo_mei'] == 'S',
        'data_opcao_mei': parse_date(row['data_opcao_mei']),
        'data_exclusao_mei': parse_date(row['data_exclusao_mei'])
    }

########################## Column Definitions (CSVs sem header) ##########################
COLS_EMPRESAS = [
    'cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_do_responsavel',
    'capital_social', 'porte_da_empresa', 'ente_federativo_responsavel'
]

COLS_ESTABELECIMENTOS = [
    'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia',
    'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral',
    'nome_da_cidade_no_exterior', 'pais', 'data_de_inicio_da_atividade',
    'cnae_fiscal_principal', 'cnae_fiscal_secundaria', 'tipo_de_logradouro', 'logradouro',
    'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd1', 'telefone1',
    'ddd2', 'telefone2', 'ddd_do_fax', 'fax', 'correio_eletronico', 'situacao_especial',
    'data_da_situacao_especial'
]

COLS_SOCIOS = [
    'cnpj_basico', 'identificador_de_socio', 'nome_do_socio', 'cnpj_ou_cpf_do_socio',
    'qualificacao_do_socio', 'data_de_entrada_sociedade', 'pais', 'representante_legal',
    'nome_do_representante', 'qualificacao_do_representante_legal', 'faixa_etaria'
]

COLS_SIMPLES = [
    'cnpj_basico', 'opcao_pelo_simples', 'data_opcao_simples', 'data_exclusao_simples',
    'opcao_pelo_mei', 'data_opcao_mei', 'data_exclusao_mei'
]

########################## Main Processing ##########################
def main():
    log("=" * 60)
    log("Iniciando conversão para JSONL (MongoDB)")
    log("=" * 60)

    # Carrega tabelas de lookup
    lookups = load_lookup_tables()

    # Paths dos arquivos principais
    empresas_path = os.path.join(path_outgoing, 'empresas.csv')
    estabelecimentos_path = os.path.join(path_outgoing, 'estabelecimentos.csv')
    socios_path = os.path.join(path_outgoing, 'socios.csv')
    simples_path = os.path.join(path_outgoing, 'simples.csv')
    output_path = os.path.join(path_outgoing, 'empresas.jsonl')

    # Verifica se arquivos existem
    for fpath, name in [(empresas_path, 'empresas'), (estabelecimentos_path, 'estabelecimentos')]:
        if not os.path.exists(fpath):
            log(f"ERRO: Arquivo {name}.csv não encontrado!")
            return

    # Carrega estabelecimentos e agrupa por cnpj_basico
    log("Carregando estabelecimentos...")
    estabelecimentos_df = pd.read_csv(
        estabelecimentos_path, sep=csv_sep, encoding=csv_enc, dtype=str,
        header=None, names=COLS_ESTABELECIMENTOS
    )
    estabelecimentos_grouped = estabelecimentos_df.groupby('cnpj_basico')
    log(f"  Estabelecimentos carregados: {len(estabelecimentos_df)} registros")
    log(f"  CNPJs únicos: {len(estabelecimentos_grouped)} empresas")
    del estabelecimentos_df  # Libera memória

    # Carrega sócios e agrupa por cnpj_basico
    log("Carregando sócios...")
    socios_grouped = {}
    if os.path.exists(socios_path):
        socios_df = pd.read_csv(
            socios_path, sep=csv_sep, encoding=csv_enc, dtype=str,
            header=None, names=COLS_SOCIOS
        )
        socios_grouped = socios_df.groupby('cnpj_basico')
        log(f"  Sócios carregados: {len(socios_df)} registros")
        del socios_df

    # Carrega simples como dicionário (este arquivo TEM header)
    log("Carregando dados do Simples Nacional...")
    simples_dict = {}
    if os.path.exists(simples_path):
        simples_df = pd.read_csv(simples_path, sep=csv_sep, encoding=csv_enc, dtype=str)
        simples_dict = {row['cnpj_basico']: row for _, row in simples_df.iterrows()}
        log(f"  Simples carregados: {len(simples_dict)} registros")
        del simples_df

    # Processa empresas em chunks e gera JSONL
    log("Processando empresas e gerando JSONL...")
    chunk_size = 50000
    total_empresas = 0

    with open(output_path, 'w', encoding='utf-8') as jsonl_file:
        for chunk in tqdm(pd.read_csv(empresas_path, sep=csv_sep, encoding=csv_enc, dtype=str, header=None, names=COLS_EMPRESAS, chunksize=chunk_size), desc="Processando"):
            for _, empresa_row in chunk.iterrows():
                cnpj_basico = empresa_row['cnpj_basico']

                # Monta documento da empresa
                doc = {
                    '_id': cnpj_basico,
                    'cnpj_basico': cnpj_basico,
                    'razao_social': empresa_row['razao_social'] if pd.notna(empresa_row['razao_social']) else None,
                    'natureza_juridica': {
                        'codigo': empresa_row['natureza_juridica'],
                        'descricao': lookups['natureza'].get(str(empresa_row['natureza_juridica']).zfill(4), None)
                    } if pd.notna(empresa_row['natureza_juridica']) else None,
                    'qualificacao_responsavel': {
                        'codigo': empresa_row['qualificacao_do_responsavel'],
                        'descricao': lookups['qualificacoes'].get(str(empresa_row['qualificacao_do_responsavel']).zfill(2), None)
                    } if pd.notna(empresa_row['qualificacao_do_responsavel']) else None,
                    'capital_social': parse_capital_social(empresa_row['capital_social']),
                    'porte': {
                        'codigo': empresa_row['porte_da_empresa'],
                        'descricao': get_porte_descricao(empresa_row['porte_da_empresa'])
                    } if pd.notna(empresa_row['porte_da_empresa']) else None,
                    'ente_federativo': empresa_row['ente_federativo_responsavel'] if pd.notna(empresa_row['ente_federativo_responsavel']) and empresa_row['ente_federativo_responsavel'] else None
                }

                # Adiciona estabelecimentos
                doc['estabelecimentos'] = []
                try:
                    estab_group = estabelecimentos_grouped.get_group(cnpj_basico)
                    for _, estab_row in estab_group.iterrows():
                        doc['estabelecimentos'].append(build_estabelecimento(estab_row, lookups))
                    # Ordena: matriz primeiro
                    doc['estabelecimentos'].sort(key=lambda x: (not x['matriz'], x['cnpj']))
                except KeyError:
                    pass

                # Adiciona sócios
                doc['socios'] = []
                if socios_grouped:
                    try:
                        socios_group = socios_grouped.get_group(cnpj_basico)
                        for _, socio_row in socios_group.iterrows():
                            doc['socios'].append(build_socio(socio_row, lookups))
                    except KeyError:
                        pass

                # Adiciona dados do Simples
                if cnpj_basico in simples_dict:
                    doc['simples'] = build_simples(simples_dict[cnpj_basico])
                else:
                    doc['simples'] = None

                # Escreve linha no JSONL
                jsonl_file.write(json.dumps(doc, ensure_ascii=False) + '\n')
                total_empresas += 1

    log("=" * 60)
    log(f"Conversão concluída!")
    log(f"Total de empresas processadas: {total_empresas}")
    log(f"Arquivo gerado: {output_path}")
    log("")
    log("Para importar no MongoDB, execute:")
    log(f"  mongoimport --db cnpj --collection empresas --file {output_path}")
    log("=" * 60)

if __name__ == '__main__':
    main()
