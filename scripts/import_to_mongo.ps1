# Script para importar o JSONL no MongoDB via Docker (Windows PowerShell)

Write-Host "==============================================" -ForegroundColor Cyan
Write-Host "Importando empresas.jsonl para o MongoDB" -ForegroundColor Cyan
Write-Host "==============================================" -ForegroundColor Cyan

# Verifica se o arquivo existe
if (-not (Test-Path "data_outgoing/empresas.jsonl")) {
    Write-Host "ERRO: Arquivo data_outgoing/empresas.jsonl nao encontrado!" -ForegroundColor Red
    Write-Host "Execute primeiro: python scripts/cnpj_to_jsonl.py" -ForegroundColor Yellow
    exit 1
}

# Verifica se o container está rodando
$container = docker ps --filter "name=cnpj-mongodb" --format "{{.Names}}"
if (-not $container) {
    Write-Host "ERRO: Container cnpj-mongodb nao esta rodando!" -ForegroundColor Red
    Write-Host "Execute primeiro: docker-compose up -d" -ForegroundColor Yellow
    exit 1
}

Write-Host ""
Write-Host "Iniciando importacao..." -ForegroundColor Green
Write-Host "Isso pode demorar alguns minutos dependendo do tamanho do arquivo..."

# Importa usando mongoimport dentro do container
docker exec -i cnpj-mongodb mongoimport `
    --uri="mongodb://admin:admin123@localhost:27017/cnpj?authSource=admin" `
    --collection=empresas `
    --drop `
    --file=/data/import/empresas.jsonl

Write-Host ""
Write-Host "Criando indices..." -ForegroundColor Green

# Cria índices usando arquivo JS
$indexScriptPath = "docker/mongo-init/create-indexes.js"

docker exec cnpj-mongodb mongosh `
    --username admin `
    --password admin123 `
    --authenticationDatabase admin `
    cnpj `
    --file /docker-entrypoint-initdb.d/create-indexes.js

Write-Host ""
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host "Importacao concluida!" -ForegroundColor Green
Write-Host ""
Write-Host "Acesse o Mongo Express em: http://localhost:8081" -ForegroundColor Yellow
Write-Host "Conexao direta: mongodb://cnpj_user:cnpj123@localhost:27017/cnpj" -ForegroundColor Yellow
Write-Host "==============================================" -ForegroundColor Cyan
