import pandas as pd
import pyarrow.parquet as pq
import os
from pathlib import Path

# Diretório com os arquivos parquet
data_dir = Path(__file__).parent.parent / "data_outgoing"

# Listar todos os arquivos parquet
parquet_files = sorted([f for f in os.listdir(data_dir) if f.endswith('.parquet')])

print("=" * 80)
print("ANÁLISE DOS ARQUIVOS PARQUET")
print("=" * 80)

for filename in parquet_files:
    filepath = data_dir / filename
    print(f"\n{'='*80}")
    print(f"ARQUIVO: {filename}")
    print(f"{'='*80}")
    
    # Ler metadados do arquivo
    parquet_file = pq.ParquetFile(filepath)
    
    # Informações gerais
    print(f"\n📊 INFORMAÇÕES GERAIS:")
    print(f"   Total de linhas: {parquet_file.metadata.num_rows:,}")
    print(f"   Total de colunas: {parquet_file.metadata.num_columns}")
    print(f"   Tamanho do arquivo: {os.path.getsize(filepath) / (1024*1024):.2f} MB")
    
    # Schema (estrutura das colunas)
    print(f"\n📋 SCHEMA (ESTRUTURA):")
    schema = parquet_file.schema
    for i in range(len(schema)):
        field = schema[i]
        print(f"   {i+1}. {field.name} - Tipo: {field.physical_type}")
    
    # Ler apenas as primeiras 5 linhas (muito mais rápido)
    print(f"\n🔍 PRIMEIROS 3 REGISTROS:")
    
    # Usar PyArrow para ler apenas as primeiras linhas de forma eficiente
    table = parquet_file.read_row_group(0)
    df_sample = table.to_pandas()[:5]  # Pegar só as primeiras 5 linhas do primeiro grupo
    
    # Mostrar os primeiros 3 registros de forma formatada
    for idx in range(min(3, len(df_sample))):
        print(f"\n   --- Registro {idx + 1} ---")
        for col in df_sample.columns:
            value = df_sample.iloc[idx][col]
            print(f"   {col}: {value}")
    
    # Informações das colunas baseadas na amostra
    print(f"\n📈 TIPOS DE DADOS DAS COLUNAS:")
    print(df_sample.dtypes.to_string())
    
    # Limpar memória
    del df_sample
    del table
    del parquet_file

print(f"\n{'='*80}")
print("ANÁLISE CONCLUÍDA")
print(f"{'='*80}")
