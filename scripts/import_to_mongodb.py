#!/usr/bin/env python3
"""
Script para importar arquivos .parquet do diretório data_outgoing para o MongoDB.
Cada arquivo .parquet será importado para uma collection com o mesmo nome.
"""

import os
import sys
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, BulkWriteError
from tqdm import tqdm
import logging
from datetime import datetime

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/mongodb_import_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configurações do MongoDB
MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_USER = os.getenv('MONGO_USER', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'admin123')
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'cnpj')

# Diretórios
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_OUTGOING_DIR = BASE_DIR / 'data_outgoing'

# Tamanho do batch para inserção
BATCH_SIZE = 10000


def connect_to_mongodb():
    """Conecta ao MongoDB e retorna o cliente e o database."""
    try:
        # String de conexão
        connection_string = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
        
        logger.info(f"Conectando ao MongoDB em {MONGO_HOST}:{MONGO_PORT}...")
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        
        # Testa a conexão
        client.admin.command('ping')
        logger.info("✓ Conectado ao MongoDB com sucesso!")
        
        db = client[MONGO_DATABASE]
        return client, db
    
    except ConnectionFailure as e:
        logger.error(f"✗ Erro ao conectar ao MongoDB: {e}")
        logger.error("Certifique-se de que o MongoDB está rodando (docker compose up -d)")
        sys.exit(1)


def get_parquet_files():
    """Retorna lista de arquivos .parquet no diretório data_outgoing."""
    if not DATA_OUTGOING_DIR.exists():
        logger.error(f"✗ Diretório não encontrado: {DATA_OUTGOING_DIR}")
        sys.exit(1)
    
    parquet_files = list(DATA_OUTGOING_DIR.glob('*.parquet'))
    
    if not parquet_files:
        logger.warning(f"✗ Nenhum arquivo .parquet encontrado em {DATA_OUTGOING_DIR}")
        sys.exit(0)
    
    logger.info(f"✓ Encontrados {len(parquet_files)} arquivos .parquet")
    return parquet_files


def import_parquet_to_mongodb(file_path: Path, db):
    """Importa um arquivo .parquet para uma collection do MongoDB."""
    collection_name = file_path.stem  # Nome do arquivo sem extensão
    collection = db[collection_name]
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Processando: {file_path.name}")
    logger.info(f"Collection: {collection_name}")
    
    try:
        # Lê informações do arquivo parquet
        parquet_file = pq.ParquetFile(file_path)
        total_rows = parquet_file.metadata.num_rows
        
        logger.info(f"Total de registros: {total_rows:,}")
        
        # Verifica se a collection já existe e tem dados
        existing_count = collection.count_documents({})
        if existing_count > 0:
            logger.warning(f"⚠ A collection '{collection_name}' já possui {existing_count:,} documentos")
            response = input("Deseja limpar a collection antes de importar? (s/N): ").lower()
            if response == 's':
                collection.delete_many({})
                logger.info(f"✓ Collection '{collection_name}' limpa")
            else:
                logger.info("Continuando sem limpar...")
        
        # Lê e processa o arquivo em chunks
        logger.info(f"Iniciando importação em batches de {BATCH_SIZE:,} registros...")
        
        total_inserted = 0
        
        # Para arquivos pequenos, lê tudo de uma vez
        if total_rows <= BATCH_SIZE:
            df = pd.read_parquet(file_path, engine='pyarrow')
            df = df.where(pd.notnull(df), None)
            records = df.to_dict('records')
            
            try:
                result = collection.insert_many(records, ordered=False)
                total_inserted = len(result.inserted_ids)
                logger.info(f"✓ {total_inserted:,} documentos inseridos")
            except BulkWriteError as bwe:
                total_inserted = bwe.details.get('nInserted', 0)
                logger.warning(f"⚠ {total_inserted:,} documentos inseridos, {len(bwe.details.get('writeErrors', []))} erros")
        
        else:
            # Para arquivos grandes, processa por batches usando pyarrow
            parquet_table = pq.read_table(file_path)
            
            # Processa em batches
            for i in range(0, total_rows, BATCH_SIZE):
                end_idx = min(i + BATCH_SIZE, total_rows)
                
                # Converte batch para pandas DataFrame
                batch_table = parquet_table.slice(i, end_idx - i)
                batch_df = batch_table.to_pandas()
                
                # Converte NaN para None (null no MongoDB)
                batch_df = batch_df.where(pd.notnull(batch_df), None)
                
                # Converte DataFrame para lista de dicionários
                records = batch_df.to_dict('records')
                
                try:
                    # Insere batch no MongoDB
                    result = collection.insert_many(records, ordered=False)
                    total_inserted += len(result.inserted_ids)
                    
                    # Atualiza progresso
                    progress = (total_inserted / total_rows) * 100
                    print(f"\rProgresso: {total_inserted:,}/{total_rows:,} ({progress:.1f}%)", end='', flush=True)
                    
                except BulkWriteError as bwe:
                    # Continua mesmo se houver erros em alguns documentos
                    total_inserted += bwe.details.get('nInserted', 0)
                    logger.warning(f"\n⚠ Alguns documentos falharam: {len(bwe.details.get('writeErrors', []))} erros")
        
        print()  # Nova linha após o progresso
        logger.info(f"✓ Importação concluída: {total_inserted:,} documentos inseridos")
        
        # Cria índices básicos (se aplicável)
        create_indexes(collection, collection_name)
        
        return total_inserted
        
    except Exception as e:
        logger.error(f"✗ Erro ao importar {file_path.name}: {e}")
        return 0


def create_indexes(collection, collection_name):
    """Cria índices úteis para cada collection."""
    try:
        logger.info("Criando índices...")
        
        # Índices específicos por collection
        if collection_name == 'empresas':
            collection.create_index('cnpj_basico')
            logger.info("✓ Índice criado: cnpj_basico")
        
        elif collection_name == 'estabelecimentos':
            collection.create_index([('cnpj_basico', 1), ('cnpj_ordem', 1), ('cnpj_dv', 1)])
            collection.create_index('situacao_cadastral')
            logger.info("✓ Índices criados: cnpj_basico+cnpj_ordem+cnpj_dv, situacao_cadastral")
        
        elif collection_name == 'socios':
            collection.create_index('cnpj_basico')
            logger.info("✓ Índice criado: cnpj_basico")
        
        elif collection_name == 'simples':
            collection.create_index('cnpj_basico')
            logger.info("✓ Índice criado: cnpj_basico")
        
        elif collection_name == 'cnaes':
            collection.create_index('codigo')
            logger.info("✓ Índice criado: codigo")
        
        elif collection_name == 'municipios':
            collection.create_index('codigo')
            logger.info("✓ Índice criado: codigo")
        
    except Exception as e:
        logger.warning(f"⚠ Erro ao criar índices: {e}")


def show_summary(db):
    """Mostra resumo das collections importadas."""
    logger.info(f"\n{'='*60}")
    logger.info("RESUMO DA IMPORTAÇÃO")
    logger.info(f"{'='*60}")
    
    collections = db.list_collection_names()
    
    for collection_name in sorted(collections):
        count = db[collection_name].count_documents({})
        logger.info(f"{collection_name:.<30} {count:>15,} documentos")
    
    logger.info(f"{'='*60}\n")


def main():
    """Função principal."""
    logger.info("="*60)
    logger.info("IMPORTAÇÃO DE DADOS PARQUET PARA MONGODB")
    logger.info("="*60)
    
    # Conecta ao MongoDB
    client, db = connect_to_mongodb()
    
    # Obtém lista de arquivos
    parquet_files = get_parquet_files()
    
    # Importa cada arquivo
    total_files = len(parquet_files)
    successful_imports = 0
    
    for idx, file_path in enumerate(parquet_files, 1):
        logger.info(f"\n[{idx}/{total_files}] Arquivo: {file_path.name}")
        
        result = import_parquet_to_mongodb(file_path, db)
        
        if result > 0:
            successful_imports += 1
    
    # Mostra resumo
    show_summary(db)
    
    # Fecha conexão
    client.close()
    
    logger.info(f"✓ Processo finalizado: {successful_imports}/{total_files} arquivos importados com sucesso")
    
    if successful_imports < total_files:
        sys.exit(1)


if __name__ == "__main__":
    main()
