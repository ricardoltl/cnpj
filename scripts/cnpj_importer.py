#!/usr/bin/env python3
"""
Script para importar dados de CNPJ diretamente dos arquivos .zip para o PostgreSQL.
Elimina a etapa intermediária de criação de arquivos .parquet.
"""

import os
import sys
import logging
import zipfile
import time
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.errors import UndefinedTable
import yaml
from tqdm import tqdm

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/cnpj_importer_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configurações do PostgreSQL (usando variáveis padrão do Postgres)
PG_HOST = os.getenv('POSTGRES_HOST', 'localhos222t')
PG_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
PG_DB = os.getenv('POSTGRES_DB', 'cnpj_db')
PG_USER = os.getenv('POSTGRES_USER', 'cnpj')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'cnpj123')

# Diretórios
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_INCOMING_DIR = BASE_DIR / 'data_incoming'
CONFIG_DIR = BASE_DIR / 'config'
CONFIG_FILE = CONFIG_DIR / 'config.yaml'

# Tamanho do batch para leitura e importação
BATCH_SIZE = int(os.getenv('PG_BATCH_SIZE', 50000))

# Ordem de importação das tabelas (respeita dependências)
IMPORT_ORDER = [
    'motivos',
    'municipios',
    'natureza',
    'qualificacoes',
    'paises',
    'empresas',
    'estabelecimentos',
    'socios',
    'simples',
    'cnaes',
]


def load_config():
    """Carrega as configurações do arquivo config.yaml."""
    try:
        with open(CONFIG_FILE, 'r') as file:
            config = yaml.safe_load(file)
        logger.info(f"✓ Configurações carregadas de {CONFIG_FILE}")
        return config
    except Exception as e:
        logger.error(f"✗ Erro ao carregar config.yaml: {e}")
        sys.exit(1)


def connect_to_postgres():
    """Conecta ao PostgreSQL e retorna a conexão."""
    max_retries = 6  # 6 tentativas (0, 5, 10, 15, 20, 25 segundos = 30 segundos total)
    retry_delay = 5  # Espera 5 segundos entre tentativas
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                logger.info(f"Tentativa {attempt + 1}/{max_retries} - Aguardando {retry_delay}s...")
                time.sleep(retry_delay)
            
            logger.info(f"Conectando ao PostgreSQL em {PG_HOST}:{PG_PORT}...")
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                database=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD,
            )
            logger.info("✓ Conectado ao PostgreSQL com sucesso!")
            return conn
        except psycopg2.Error as e:
            if attempt < max_retries - 1:
                logger.warning(f"⚠ Falha ao conectar (tentativa {attempt + 1}/{max_retries}): {e}")
            else:
                logger.error(f"✗ Erro ao conectar ao PostgreSQL após {max_retries} tentativas: {e}")
                logger.error("Certifique-se de que o PostgreSQL está rodando (docker compose up -d)")
                sys.exit(1)


def ensure_extensions(conn):
    """Garante extensões necessárias."""
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    conn.commit()
    logger.info("✓ Extensões PostgreSQL verificadas")


def get_table_schema_from_config(config, table):
    """Retorna o schema (colunas, tipos e índices) de uma tabela a partir do config."""
    db_structure = config.get('database_structure', {})
    if table not in db_structure:
        return None, None, None
    
    table_def = db_structure[table]
    columns = list(table_def.keys())
    types = {col: table_def[col].get('type', 'TEXT') for col in columns}
    indexes = {col: table_def[col].get('index', False) for col in columns}
    
    return columns, types, indexes


def create_or_validate_table(conn, table, columns, types):
    """Cria a tabela se não existir ou valida o schema se já existir."""
    with conn.cursor() as cur:
        # Verifica se a tabela existe
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (table,),
        )
        existing = cur.fetchall()
    
    if not existing:
        # Tabela não existe, cria
        cols_sql = ", ".join([f"{col} {types[col]}" for col in columns])
        with conn.cursor() as cur:
            cur.execute(sql.SQL(f"CREATE TABLE {table} ({cols_sql});"))
        conn.commit()
        logger.info(f"✓ Tabela '{table}' criada com sucesso")
        return True
    
    # Valida schema existente
    existing_cols = [row[0] for row in existing]
    
    if existing_cols == columns:
        logger.info(f"✓ Schema da tabela '{table}' validado")
        return True
    
    logger.warning(f"⚠ Schema da tabela '{table}' não corresponde ao esperado")
    logger.warning(f"  Esperado: {columns}")
    logger.warning(f"  Atual:    {existing_cols}")
    
    force_recreate = os.getenv('PG_FORCE_RECREATE', 'true').lower() in {'1', 'true', 'yes', 's'}
    if force_recreate:
        response = 's'
    else:
        response = input("Deseja recriar a tabela com o schema correto? (s/N): ").lower()
    
    if response == 's':
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(table)))
            cols_sql = ", ".join([f"{col} {types[col]}" for col in columns])
            cur.execute(sql.SQL(f"CREATE TABLE {table} ({cols_sql});"))
        conn.commit()
        logger.info(f"✓ Tabela '{table}' recriada")
        return True
    
    logger.error(f"✗ Importação abortada para '{table}' (schema incompatível)")
    return False


def maybe_truncate_table(conn, table):
    """Pergunta se deve limpar a tabela caso já tenha dados."""
    with conn.cursor() as cur:
        try:
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {};").format(sql.Identifier(table)))
            count = cur.fetchone()[0]
        except UndefinedTable:
            return
    
    if count > 0:
        logger.warning(f"⚠ A tabela '{table}' já possui {count:,} registros")
        
        force_truncate = os.getenv('PG_FORCE_TRUNCATE', 'true').lower() in {'1', 'true', 'yes', 's'}
        if force_truncate:
            response = 's'
        else:
            response = input("Deseja limpar a tabela antes de importar? (s/N): ").lower()
        
        if response == 's':
            with conn.cursor() as cur:
                cur.execute(sql.SQL("TRUNCATE TABLE {};").format(sql.Identifier(table)))
            conn.commit()
            logger.info(f"✓ Tabela '{table}' limpa")
        else:
            logger.info("Continuando sem limpar...")


def copy_batch(conn, table, df, columns):
    """Copia um batch para o PostgreSQL via COPY."""
    # Substitui NaN por None para compatibilidade com PostgreSQL
    df = df.where(pd.notnull(df), None)
    
    buffer = StringIO()
    df.to_csv(
        buffer,
        index=False,
        header=False,
        sep='\t',
        na_rep='\\N',
        quoting=0,
        escapechar='\\'
    )
    buffer.seek(0)
    
    with conn.cursor() as cur:
        copy_sql = sql.SQL(
            "COPY {} ({}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N', ESCAPE '\\');"
        ).format(
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )
        cur.copy_expert(copy_sql.as_string(conn), buffer)


def find_zip_files_for_table(table):
    """Encontra todos os arquivos .zip relacionados a uma tabela."""
    if not DATA_INCOMING_DIR.exists():
        logger.error(f"✗ Diretório não encontrado: {DATA_INCOMING_DIR}")
        return []
    
    zip_files = []
    table_prefix = table.title()
    
    for root, directories, files in os.walk(DATA_INCOMING_DIR):
        for filename in files:
            if filename.endswith(".zip"):
                file_without_ext = filename.split('.')[0]
                if file_without_ext.startswith(table_prefix):
                    zip_file_path = Path(root) / filename
                    zip_files.append(zip_file_path)
    
    return sorted(zip_files)


def import_zip_to_postgres(zip_file_path, conn, table, columns, config):
    """Importa dados de um arquivo .zip diretamente para o PostgreSQL."""
    csv_sep = config['csv_sep']
    csv_dec = config['csv_dec']
    csv_quote = config['csv_quote']
    csv_enc = config['csv_enc']
    
    # Cria dtypes dict para pandas (string type)
    dtypes = {col: 'string' for col in columns}
    
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_file_list = zip_ref.namelist()
            
            if len(zip_file_list) != 1:
                logger.error(f"✗ ZIP contém {len(zip_file_list)} arquivos (esperado: 1)")
                return 0
            
            csv_filename = zip_file_list[0]
            logger.info(f"  Processando: {zip_file_path.name} -> {csv_filename}")
            
            with zip_ref.open(csv_filename) as csvfile:
                # Lê o CSV em chunks para não sobrecarregar a memória
                total_inserted = 0
                
                for chunk in pd.read_csv(
                    csvfile,
                    header=None,
                    names=columns,
                    dtype=dtypes,
                    sep=csv_sep,
                    decimal=csv_dec,
                    quotechar=csv_quote,
                    encoding=csv_enc,
                    low_memory=False,
                    chunksize=BATCH_SIZE
                ):
                    copy_batch(conn, table, chunk, columns)
                    total_inserted += len(chunk)
                    print(f"\r  Registros importados: {total_inserted:,}", end='', flush=True)
                
                print()  # Nova linha após o progresso
                return total_inserted
    
    except Exception as e:
        logger.error(f"✗ Erro ao processar {zip_file_path.name}: {e}")
        return 0


def import_table(conn, table, config):
    """Importa todos os dados de uma tabela a partir dos arquivos .zip."""
    logger.info(f"\n{'='*60}")
    logger.info(f"Processando tabela: {table}")
    logger.info(f"{'='*60}")
    
    # Obtém schema da tabela do config
    columns, types, indexes = get_table_schema_from_config(config, table)
    
    if not columns:
        logger.warning(f"⚠ Tabela '{table}' não encontrada no config.yaml")
        return 0
    
    # Cria ou valida a tabela
    if not create_or_validate_table(conn, table, columns, types):
        return 0
    
    # Pergunta se deve truncar
    maybe_truncate_table(conn, table)
    
    # Encontra arquivos .zip
    zip_files = find_zip_files_for_table(table)
    
    if not zip_files:
        logger.warning(f"⚠ Nenhum arquivo .zip encontrado para a tabela '{table}'")
        return 0
    
    logger.info(f"✓ Encontrados {len(zip_files)} arquivo(s) .zip para '{table}'")
    
    # Importa cada arquivo
    total_inserted = 0
    
    for zip_file in zip_files:
        inserted = import_zip_to_postgres(zip_file, conn, table, columns, config)
        total_inserted += inserted
        conn.commit()
    
    logger.info(f"✓ Importação da tabela '{table}' concluída: {total_inserted:,} registros inseridos")
    
    return total_inserted


def create_indexes(conn, config):
    """Cria índices baseados na configuração do database_structure."""
    logger.info(f"\n{'='*60}")
    logger.info("Criando índices...")
    logger.info(f"{'='*60}")
    
    db_structure = config.get('database_structure', {})
    
    with conn.cursor() as cur:
        for table, table_def in db_structure.items():
            for column, col_def in table_def.items():
                if col_def.get('index', False):
                    index_name = f"idx_{table}_{column}"
                    try:
                        # Verifica se é coluna de texto para criar índice GIN para busca fuzzy em colunas específicas
                        if column == 'razao_social' and table == 'empresas':
                            cur.execute(
                                sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} USING gin ({} gin_trgm_ops);").format(
                                    sql.Identifier(index_name + '_trgm'),
                                    sql.Identifier(table),
                                    sql.Identifier(column)
                                )
                            )
                            logger.info(f"  ✓ Índice GIN criado: {index_name}_trgm")
                        
                        # Índice normal
                        cur.execute(
                            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({});").format(
                                sql.Identifier(index_name),
                                sql.Identifier(table),
                                sql.Identifier(column)
                            )
                        )
                        logger.info(f"  ✓ Índice criado: {index_name}")
                    except Exception as e:
                        logger.warning(f"  ⚠ Erro ao criar índice {index_name}: {e}")
        
        # Índice composto para estabelecimentos (CNPJ completo)
        try:
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_estab_cnpj_composto ON estabelecimentos (cnpj_basico, cnpj_ordem, cnpj_dv);"
            )
            logger.info(f"  ✓ Índice composto criado: idx_estab_cnpj_composto")
        except Exception as e:
            logger.warning(f"  ⚠ Erro ao criar índice composto: {e}")
    
    conn.commit()
    logger.info("✓ Índices criados com sucesso")


def show_summary(conn):
    """Mostra resumo das tabelas importadas."""
    logger.info(f"\n{'='*60}")
    logger.info("RESUMO DA IMPORTAÇÃO")
    logger.info(f"{'='*60}")
    
    with conn.cursor() as cur:
        for table in IMPORT_ORDER:
            try:
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {};").format(sql.Identifier(table)))
                count = cur.fetchone()[0]
                logger.info(f"{table:.<30} {count:>15,} registros")
            except Exception as e:
                logger.warning(f"{table:.<30} {'ERRO':>15}")
    
    logger.info(f"{'='*60}\n")


def main():
    """Função principal."""
    logger.info("=" * 60)
    logger.info("IMPORTAÇÃO DIRETA DE DADOS CNPJ (.ZIP -> POSTGRESQL)")
    logger.info("=" * 60)
    
    # Carrega configurações
    config = load_config()
    
    # Conecta ao PostgreSQL
    conn = connect_to_postgres()
    
    # Garante extensões
    ensure_extensions(conn)
    
    # Importa cada tabela na ordem correta
    successful_imports = 0
    
    for table in IMPORT_ORDER:
        try:
            result = import_table(conn, table, config)
            if result > 0:
                successful_imports += 1
        except Exception as e:
            logger.error(f"✗ Erro ao importar tabela '{table}': {e}")
            conn.rollback()
    
    # Cria índices
    try:
        create_indexes(conn, config)
    except Exception as e:
        logger.error(f"✗ Erro ao criar índices: {e}")
    
    # Mostra resumo
    show_summary(conn)
    
    # Fecha conexão
    conn.close()
    
    logger.info(f"✓ Processo finalizado: {successful_imports}/{len(IMPORT_ORDER)} tabelas importadas com sucesso")
    
    if successful_imports < len(IMPORT_ORDER):
        sys.exit(1)


if __name__ == '__main__':
    main()
