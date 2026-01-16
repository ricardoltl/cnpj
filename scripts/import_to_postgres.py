"""
Importador CSV -> PostgreSQL
Usa COPY para importação rápida e eficiente (sem consumir RAM)
"""
import os
import subprocess
import time

########################## Configurações ##########################
path_script = os.path.abspath(__file__)
path_script_dir = os.path.dirname(path_script)
path_project = os.path.dirname(path_script_dir)
path_outgoing = os.path.join(path_project, 'data_outgoing')

# PostgreSQL
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "cnpj_db"
PG_USER = "cnpj"
PG_PASSWORD = "cnpj123"

def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}")

def run_sql(sql, use_psql=False):
    """Executa SQL via psql no container"""
    env = os.environ.copy()
    env['PGPASSWORD'] = PG_PASSWORD

    cmd = [
        'docker', 'exec', '-i', 'cnpj-postgres',
        'psql', '-U', PG_USER, '-d', PG_DB, '-c', sql
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        log(f"  ERRO: {result.stderr}")
    return result.returncode == 0

def run_sql_file(sql):
    """Executa SQL via docker exec"""
    env = os.environ.copy()
    env['PGPASSWORD'] = PG_PASSWORD

    cmd = [
        'docker', 'exec', '-i', 'cnpj-postgres',
        'psql', '-U', PG_USER, '-d', PG_DB
    ]

    result = subprocess.run(cmd, input=sql, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        log(f"  ERRO: {result.stderr}")
    return result.returncode == 0

def import_csv_via_docker(table_name, csv_file, columns, has_header=True):
    """Importa CSV usando COPY dentro do container"""
    header_opt = "HEADER," if has_header else ""

    sql = f"""
    COPY {table_name} ({columns})
    FROM '/data/{csv_file}'
    WITH (FORMAT csv, DELIMITER ';', {header_opt} ENCODING 'LATIN1', QUOTE '"', NULL '');
    """

    return run_sql_file(sql)

def main():
    log("=" * 60)
    log("Importador CSV -> PostgreSQL")
    log("=" * 60)

    # Cria tabelas
    log("")
    log("Criando tabelas...")

    create_tables_sql = """
    -- Extensão para busca fuzzy
    CREATE EXTENSION IF NOT EXISTS pg_trgm;

    -- Tabelas de lookup
    DROP TABLE IF EXISTS cnaes CASCADE;
    CREATE TABLE cnaes (
        codigo VARCHAR(10) PRIMARY KEY,
        descricao TEXT
    );

    DROP TABLE IF EXISTS municipios CASCADE;
    CREATE TABLE municipios (
        codigo VARCHAR(10) PRIMARY KEY,
        nome VARCHAR(255)
    );

    DROP TABLE IF EXISTS naturezas CASCADE;
    CREATE TABLE naturezas (
        codigo VARCHAR(10) PRIMARY KEY,
        descricao TEXT
    );

    DROP TABLE IF EXISTS qualificacoes CASCADE;
    CREATE TABLE qualificacoes (
        codigo VARCHAR(10) PRIMARY KEY,
        descricao TEXT
    );

    DROP TABLE IF EXISTS paises CASCADE;
    CREATE TABLE paises (
        codigo VARCHAR(10) PRIMARY KEY,
        nome VARCHAR(255)
    );

    DROP TABLE IF EXISTS motivos CASCADE;
    CREATE TABLE motivos (
        codigo VARCHAR(10) PRIMARY KEY,
        descricao TEXT
    );

    -- Tabelas principais
    DROP TABLE IF EXISTS empresas CASCADE;
    CREATE TABLE empresas (
        cnpj_basico VARCHAR(8) PRIMARY KEY,
        razao_social TEXT,
        natureza_juridica VARCHAR(10),
        qualificacao_responsavel VARCHAR(10),
        capital_social VARCHAR(20),
        porte VARCHAR(2),
        ente_federativo VARCHAR(255)
    );

    DROP TABLE IF EXISTS estabelecimentos CASCADE;
    CREATE TABLE estabelecimentos (
        cnpj_basico VARCHAR(8),
        cnpj_ordem VARCHAR(4),
        cnpj_dv VARCHAR(2),
        matriz_filial VARCHAR(1),
        nome_fantasia TEXT,
        situacao_cadastral VARCHAR(2),
        data_situacao_cadastral VARCHAR(8),
        motivo_situacao VARCHAR(10),
        cidade_exterior VARCHAR(255),
        pais VARCHAR(10),
        data_inicio_atividade VARCHAR(8),
        cnae_principal VARCHAR(10),
        cnae_secundario TEXT,
        tipo_logradouro VARCHAR(50),
        logradouro TEXT,
        numero VARCHAR(20),
        complemento TEXT,
        bairro VARCHAR(255),
        cep VARCHAR(10),
        uf VARCHAR(2),
        municipio VARCHAR(10),
        ddd1 VARCHAR(5),
        telefone1 VARCHAR(20),
        ddd2 VARCHAR(5),
        telefone2 VARCHAR(20),
        ddd_fax VARCHAR(5),
        fax VARCHAR(20),
        email TEXT,
        situacao_especial TEXT,
        data_situacao_especial VARCHAR(8),
        PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
    );

    DROP TABLE IF EXISTS socios CASCADE;
    CREATE TABLE socios (
        id SERIAL PRIMARY KEY,
        cnpj_basico VARCHAR(8),
        tipo_socio VARCHAR(1),
        nome TEXT,
        documento VARCHAR(20),
        qualificacao VARCHAR(10),
        data_entrada VARCHAR(8),
        pais VARCHAR(10),
        representante_legal VARCHAR(20),
        nome_representante TEXT,
        qualificacao_representante VARCHAR(10),
        faixa_etaria VARCHAR(2)
    );

    DROP TABLE IF EXISTS simples CASCADE;
    CREATE TABLE simples (
        cnpj_basico VARCHAR(8) PRIMARY KEY,
        opcao_simples VARCHAR(1),
        data_opcao_simples VARCHAR(8),
        data_exclusao_simples VARCHAR(8),
        opcao_mei VARCHAR(1),
        data_opcao_mei VARCHAR(8),
        data_exclusao_mei VARCHAR(8)
    );
    """

    run_sql_file(create_tables_sql)
    log("  Tabelas criadas!")

    # Importa lookups
    log("")
    log("=" * 60)
    log("Importando tabelas de lookup...")
    log("=" * 60)

    lookups = [
        ('cnaes', 'cnaes.csv', 'codigo, descricao'),
        ('municipios', 'municipios.csv', 'codigo, nome'),
        ('naturezas', 'natureza.csv', 'codigo, descricao'),
        ('qualificacoes', 'qualificacoes.csv', 'codigo, descricao'),
        ('paises', 'paises.csv', 'codigo, nome'),
        ('motivos', 'motivos.csv', 'codigo, descricao'),
    ]

    for table, csv_file, columns in lookups:
        csv_path = os.path.join(path_outgoing, csv_file)
        if os.path.exists(csv_path):
            log(f"  {table}...")
            start = time.time()
            import_csv_via_docker(table, csv_file, columns, has_header=True)
            log(f"    OK ({time.time() - start:.1f}s)")
        else:
            log(f"  {table}: arquivo não encontrado")

    # Importa tabelas principais
    log("")
    log("=" * 60)
    log("Importando tabelas principais...")
    log("=" * 60)

    # Empresas
    log("  empresas...")
    start = time.time()
    import_csv_via_docker(
        'empresas', 'empresas.csv',
        'cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte, ente_federativo',
        has_header=True
    )
    log(f"    OK ({time.time() - start:.1f}s)")

    # Estabelecimentos
    log("  estabelecimentos...")
    start = time.time()
    import_csv_via_docker(
        'estabelecimentos', 'estabelecimentos.csv',
        'cnpj_basico, cnpj_ordem, cnpj_dv, matriz_filial, nome_fantasia, situacao_cadastral, data_situacao_cadastral, motivo_situacao, cidade_exterior, pais, data_inicio_atividade, cnae_principal, cnae_secundario, tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio, ddd1, telefone1, ddd2, telefone2, ddd_fax, fax, email, situacao_especial, data_situacao_especial',
        has_header=True
    )
    log(f"    OK ({time.time() - start:.1f}s)")

    # Sócios
    log("  socios...")
    start = time.time()
    import_csv_via_docker(
        'socios', 'socios.csv',
        'cnpj_basico, tipo_socio, nome, documento, qualificacao, data_entrada, pais, representante_legal, nome_representante, qualificacao_representante, faixa_etaria',
        has_header=True
    )
    log(f"    OK ({time.time() - start:.1f}s)")

    # Simples
    log("  simples...")
    start = time.time()
    import_csv_via_docker(
        'simples', 'simples.csv',
        'cnpj_basico, opcao_simples, data_opcao_simples, data_exclusao_simples, opcao_mei, data_opcao_mei, data_exclusao_mei',
        has_header=True
    )
    log(f"    OK ({time.time() - start:.1f}s)")

    # Cria índices
    log("")
    log("=" * 60)
    log("Criando índices...")
    log("=" * 60)

    indexes_sql = """
    -- Índice para busca fuzzy por razão social (pg_trgm)
    CREATE INDEX IF NOT EXISTS idx_empresas_razao_trgm
    ON empresas USING gin(razao_social gin_trgm_ops);

    -- Índice para busca por natureza jurídica
    CREATE INDEX IF NOT EXISTS idx_empresas_natureza
    ON empresas(natureza_juridica);

    -- Índices para estabelecimentos
    CREATE INDEX IF NOT EXISTS idx_estab_cnpj_basico
    ON estabelecimentos(cnpj_basico);

    CREATE INDEX IF NOT EXISTS idx_estab_cnae
    ON estabelecimentos(cnae_principal);

    CREATE INDEX IF NOT EXISTS idx_estab_uf
    ON estabelecimentos(uf);

    CREATE INDEX IF NOT EXISTS idx_estab_situacao
    ON estabelecimentos(situacao_cadastral);

    CREATE INDEX IF NOT EXISTS idx_estab_municipio
    ON estabelecimentos(municipio);

    -- Índice composto para buscas frequentes
    CREATE INDEX IF NOT EXISTS idx_estab_uf_situacao_cnae
    ON estabelecimentos(uf, situacao_cadastral, cnae_principal);

    -- Índice para busca fuzzy por nome fantasia
    CREATE INDEX IF NOT EXISTS idx_estab_nome_fantasia_trgm
    ON estabelecimentos USING gin(nome_fantasia gin_trgm_ops);

    -- Índices para sócios
    CREATE INDEX IF NOT EXISTS idx_socios_cnpj_basico
    ON socios(cnpj_basico);

    CREATE INDEX IF NOT EXISTS idx_socios_nome_trgm
    ON socios USING gin(nome gin_trgm_ops);

    -- Atualiza estatísticas
    ANALYZE;
    """

    log("  Criando índices (pode demorar alguns minutos)...")
    start = time.time()
    run_sql_file(indexes_sql)
    log(f"  Índices criados! ({time.time() - start:.1f}s)")

    # Finaliza
    log("")
    log("=" * 60)
    log("Importação concluída!")
    log("=" * 60)
    log("")
    log("Acesse o pgAdmin em: http://localhost:8080")
    log("  Email: admin@admin.com")
    log("  Senha: admin123")
    log("")
    log("Conexão PostgreSQL:")
    log("  Host: localhost")
    log("  Porta: 5432")
    log("  Database: cnpj_db")
    log("  User: cnpj")
    log("  Password: cnpj123")
    log("")
    log("Exemplo de busca fuzzy:")
    log("  SELECT e.razao_social, est.telefone1, est.email")
    log("  FROM empresas e")
    log("  JOIN estabelecimentos est ON e.cnpj_basico = est.cnpj_basico")
    log("  WHERE e.razao_social % 'ACADEMIA SMART FIT'")
    log("  ORDER BY similarity(e.razao_social, 'ACADEMIA SMART FIT') DESC")
    log("  LIMIT 10;")

if __name__ == '__main__':
    main()
