#!/bin/sh

set -e

echo "====================================="
echo "CNPJ Setup - Iniciando..."
echo "====================================="

echo ""
echo "[1/3] Instalando dependências..."
uv pip install --system -r pyproject.toml

echo ""
echo "[2/3] Extraindo dados da Receita Federal..."
python3 scripts/cnpj_extractor.py

echo ""
echo "[3/3] Importando dados para o PostgreSQL..."
python3 scripts/cnpj_importer.py

echo ""
echo "====================================="
echo "Setup concluído com sucesso!"
echo "====================================="
