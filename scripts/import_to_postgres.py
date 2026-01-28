#!/usr/bin/env python3
"""
Script para importar arquivos .parquet do diretório data_outgoing para o PostgreSQL.
Cada arquivo .parquet será importado para uma tabela com o mesmo nome.
"""

import os
import sys
import logging
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import psycopg2
from psycopg2 import sql
from psycopg2.errors import UndefinedTable

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/postgres_import_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configurações do PostgreSQL
PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = int(os.getenv('PG_PORT', 5432))
PG_DB = os.getenv('PG_DB', 'cnpj_db')
PG_USER = os.getenv('PG_USER', 'cnpj')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'cnpj123')

# Diretórios
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_OUTGOING_DIR = BASE_DIR / 'data_outgoing'

# Tamanho do batch para importação
BATCH_SIZE = int(os.getenv('PG_BATCH_SIZE', 50000))

# Esquemas das tabelas (colunas em ordem)
TABLE_SCHEMAS = {
    'motivos': [
        'codigo_motivo',
        'descricao_motivo',
    ],
    'municipios': [
        'codigo_municipio',
        'nome_municipio',
    ],
    'natureza': [
        'codigo_natureza_juridica',
        'descricao_natureza_juridica',
    ],
    'qualificacoes': [
        'codigo_qualificacao',
        'descricao_qualificacao',
    ],
    'paises': [
        'codigo_pais',
        'nome_pais',
    ],
    'empresas': [
        'cnpj_basico',
        'razao_social',
        'natureza_juridica',
        'qualificacao_do_responsavel',
        'capital_social',
        'porte_da_empresa',
        'ente_federativo_responsavel',
    ],
    'estabelecimentos': [
        'cnpj_basico',
        'cnpj_ordem',
        'cnpj_dv',
        'identificador_matriz_filial',
        'nome_fantasia',
        'situacao_cadastral',
        'data_situacao_cadastral',
        'motivo_situacao_cadastral',
        'nome_da_cidade_no_exterior',
        'pais',
        'data_de_inicio_da_atividade',
        'cnae_fiscal_principal',
        'cnae_fiscal_secundaria',
        'tipo_de_logradouro',
        'logradouro',
        'numero',
        'complemento',
        'bairro',
        'cep',
        'uf',
        'municipio',
        'ddd1',
        'telefone1',
        'ddd2',
        'telefone2',
        'ddd_do_fax',
        'fax',
        'correio_eletronico',
        'situacao_especial',
        'data_da_situacao_especial',
    ],
    'socios': [
        'cnpj_basico',
        'identificador_de_socio',
        'nome_do_socio',
        'cnpj_ou_cpf_do_socio',
        'qualificacao_do_socio',
        'data_de_entrada_sociedade',
        'pais',
        'representante_legal',
        'nome_do_representante',
        'qualificacao_do_representante_legal',
        'faixa_etaria',
    ],
    'simples': [
        'cnpj_basico',
        'opcao_pelo_simples',
        'data_opcao_simples',
        'data_exclusao_simples',
        'opcao_pelo_mei',
        'data_opcao_mei',
        'data_exclusao_mei',
    ],
    'cnaes': [
        'cnpj_basico',
        'cnae_fiscal_secundaria',
    ],
}

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


def connect_to_postgres():
    """Conecta ao PostgreSQL e retorna a conexão."""
    try:
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
        logger.error(f"✗ Erro ao conectar ao PostgreSQL: {e}")
        logger.error("Certifique-se de que o PostgreSQL está rodando (docker compose up -d)")
        sys.exit(1)


def get_parquet_files():
    """Retorna dict de arquivos .parquet no diretório data_outgoing por tabela."""
    if not DATA_OUTGOING_DIR.exists():
        logger.error(f"✗ Diretório não encontrado: {DATA_OUTGOING_DIR}")
        sys.exit(1)

    parquet_files = {p.stem: p for p in DATA_OUTGOING_DIR.glob('*.parquet')}

    if not parquet_files:
        logger.warning(f"✗ Nenhum arquivo .parquet encontrado em {DATA_OUTGOING_DIR}")
        sys.exit(0)

    logger.info(f"✓ Encontrados {len(parquet_files)} arquivos .parquet")
    return parquet_files


def ensure_extensions(conn):
    """Garante extensões necessárias."""
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    conn.commit()


def create_tables(conn):
    """Cria as tabelas necessárias se não existirem."""
    with conn.cursor() as cur:
        for table, columns in TABLE_SCHEMAS.items():
            cols_sql = ", ".join([f"{col} TEXT" for col in columns])
            cur.execute(f"CREATE TABLE IF NOT EXISTS {table} ({cols_sql});")
    conn.commit()


def ensure_table_schema(conn, table, columns):
    """Garante que a tabela existe com o schema esperado."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (table,),
        )
        existing = [row[0] for row in cur.fetchall()]

    if not existing:
        cols_sql = ", ".join([f"{col} TEXT" for col in columns])
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {table} ({cols_sql});")
        conn.commit()
        return True

    if existing == columns:
        return True

    logger.warning(f"⚠ Schema da tabela '{table}' não bate com o esperado.")
    logger.warning(f"  Esperado: {columns}")
    logger.warning(f"  Atual:    {existing}")

    force_recreate = os.getenv('PG_FORCE_RECREATE', '').lower() in {'1', 'true', 'yes', 's'}
    if force_recreate:
        response = 's'
    else:
        response = input("Deseja recriar a tabela com o schema correto? (s/N): ").lower()

    if response == 's':
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(table)))
            cols_sql = ", ".join([f"{col} TEXT" for col in columns])
            cur.execute(sql.SQL("CREATE TABLE {} ({});").format(sql.Identifier(table), sql.SQL(cols_sql)))
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


def import_parquet_to_postgres(file_path: Path, conn):
    """Importa um arquivo .parquet para uma tabela do PostgreSQL."""
    table = file_path.stem
    columns = TABLE_SCHEMAS.get(table)

    if not columns:
        logger.warning(f"⚠ Arquivo ignorado (sem schema conhecido): {file_path.name}")
        return 0

    logger.info(f"\n{'='*60}")
    logger.info(f"Processando: {file_path.name}")
    logger.info(f"Tabela: {table}")

    try:
        parquet_file = pq.ParquetFile(file_path)
        total_rows = parquet_file.metadata.num_rows
        logger.info(f"Total de registros: {total_rows:,}")

        if not ensure_table_schema(conn, table, columns):
            return 0

        maybe_truncate_table(conn, table)

        total_inserted = 0

        for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
            df = batch.to_pandas()
            copy_batch(conn, table, df, columns)
            total_inserted += len(df)

            progress = (total_inserted / total_rows) * 100 if total_rows else 100
            print(f"\rProgresso: {total_inserted:,}/{total_rows:,} ({progress:.1f}%)", end='', flush=True)

        print()
        conn.commit()
        logger.info(f"✓ Importação concluída: {total_inserted:,} registros inseridos")
        return total_inserted

    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Erro ao importar {file_path.name}: {e}")
        return 0


def create_indexes(conn):
    """Cria índices úteis para performance de consultas."""
    with conn.cursor() as cur:
        # Índices básicos para joins e filtros
        cur.execute("CREATE INDEX IF NOT EXISTS idx_empresas_cnpj_basico ON empresas (cnpj_basico);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_empresas_natureza ON empresas (natureza_juridica);")

        cur.execute("CREATE INDEX IF NOT EXISTS idx_estab_cnpj_basico ON estabelecimentos (cnpj_basico);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_estab_cnpj_composto ON estabelecimentos (cnpj_basico, cnpj_ordem, cnpj_dv);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_estab_cnae_principal ON estabelecimentos (cnae_fiscal_principal);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_estab_situacao ON estabelecimentos (situacao_cadastral);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_estab_uf ON estabelecimentos (uf);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_estab_municipio ON estabelecimentos (municipio);")

        cur.execute("CREATE INDEX IF NOT EXISTS idx_socios_cnpj_basico ON socios (cnpj_basico);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_simples_cnpj_basico ON simples (cnpj_basico);")

        cur.execute("CREATE INDEX IF NOT EXISTS idx_cnaes_cnpj_basico ON cnaes (cnpj_basico);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_cnaes_secundaria ON cnaes (cnae_fiscal_secundaria);")

        cur.execute("CREATE INDEX IF NOT EXISTS idx_motivos_codigo ON motivos (codigo_motivo);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_municipios_codigo ON municipios (codigo_municipio);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_natureza_codigo ON natureza (codigo_natureza_juridica);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_qualificacoes_codigo ON qualificacoes (codigo_qualificacao);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_paises_codigo ON paises (codigo_pais);")

        # Índice GIN para busca fuzzy
        cur.execute("CREATE INDEX IF NOT EXISTS idx_empresas_razao_social_trgm ON empresas USING gin (razao_social gin_trgm_ops);")

    conn.commit()


def show_summary(conn):
    """Mostra resumo das tabelas importadas."""
    logger.info(f"\n{'='*60}")
    logger.info("RESUMO DA IMPORTAÇÃO")
    logger.info(f"{'='*60}")

    with conn.cursor() as cur:
        for table in IMPORT_ORDER:
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {};").format(sql.Identifier(table)))
            count = cur.fetchone()[0]
            logger.info(f"{table:.<30} {count:>15,} registros")

    logger.info(f"{'='*60}\n")


def main():
    """Função principal."""
    logger.info("=" * 60)
    logger.info("IMPORTAÇÃO DE DADOS PARQUET PARA POSTGRESQL")
    logger.info("=" * 60)

    conn = connect_to_postgres()

    ensure_extensions(conn)
    create_tables(conn)

    parquet_files = get_parquet_files()

    total_files = 0
    successful_imports = 0

    for table in IMPORT_ORDER:
        file_path = parquet_files.get(table)
        if not file_path:
            logger.warning(f"⚠ Arquivo não encontrado para a tabela '{table}'")
            continue
        total_files += 1
        logger.info(f"\n[{total_files}] Arquivo: {file_path.name}")

        result = import_parquet_to_postgres(file_path, conn)
        if result > 0:
            successful_imports += 1

    create_indexes(conn)
    show_summary(conn)

    conn.close()

    logger.info(f"✓ Processo finalizado: {successful_imports}/{total_files} arquivos importados com sucesso")

    if successful_imports < total_files:
        sys.exit(1)


if __name__ == '__main__':
    main()
