#!/bin/bash
# Script para importar o JSONL no MongoDB via Docker

echo "=============================================="
echo "Importando empresas.jsonl para o MongoDB"
echo "=============================================="

# Verifica se o arquivo existe
if [ ! -f "data_outgoing/empresas.jsonl" ]; then
    echo "ERRO: Arquivo data_outgoing/empresas.jsonl não encontrado!"
    echo "Execute primeiro: python scripts/cnpj_to_jsonl.py"
    exit 1
fi

# Verifica se o container está rodando
if ! docker ps | grep -q cnpj-mongodb; then
    echo "ERRO: Container cnpj-mongodb não está rodando!"
    echo "Execute primeiro: docker-compose up -d"
    exit 1
fi

echo "Iniciando importação..."
echo "Isso pode demorar alguns minutos dependendo do tamanho do arquivo..."

# Importa usando mongoimport dentro do container
docker exec -i cnpj-mongodb mongoimport \
    --uri="mongodb://admin:admin123@localhost:27017/cnpj?authSource=admin" \
    --collection=empresas \
    --drop \
    --file=/data/import/empresas.jsonl

echo ""
echo "Criando índices..."

# Cria índices para otimizar queries
docker exec cnpj-mongodb mongosh \
    --username admin \
    --password admin123 \
    --authenticationDatabase admin \
    cnpj \
    --eval '
        print("Criando índice por razao_social...");
        db.empresas.createIndex({ "razao_social": "text" });

        print("Criando índice por UF...");
        db.empresas.createIndex({ "estabelecimentos.endereco.uf": 1 });

        print("Criando índice por município...");
        db.empresas.createIndex({ "estabelecimentos.endereco.municipio.codigo": 1 });

        print("Criando índice por CNAE principal...");
        db.empresas.createIndex({ "estabelecimentos.cnae_principal.codigo": 1 });

        print("Criando índice por situação cadastral...");
        db.empresas.createIndex({ "estabelecimentos.situacao_cadastral.codigo": 1 });

        print("Criando índice por CNPJ completo do estabelecimento...");
        db.empresas.createIndex({ "estabelecimentos.cnpj": 1 });

        print("Criando índice por nome do sócio...");
        db.empresas.createIndex({ "socios.nome": 1 });

        print("Criando índice composto UF + situação...");
        db.empresas.createIndex({
            "estabelecimentos.endereco.uf": 1,
            "estabelecimentos.situacao_cadastral.codigo": 1
        });

        print("");
        print("Índices criados com sucesso!");
        print("");
        print("Estatísticas da coleção:");
        printjson(db.empresas.stats());
    '

echo ""
echo "=============================================="
echo "Importação concluída!"
echo ""
echo "Acesse o Mongo Express em: http://localhost:8081"
echo "Conexão direta: mongodb://cnpj_user:cnpj123@localhost:27017/cnpj"
echo "=============================================="
