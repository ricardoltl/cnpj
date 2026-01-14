// Script para criar índices na coleção empresas
print("Criando indice por razao_social (text search)...");
db.empresas.createIndex({ "razao_social": "text" });

print("Criando indice por UF...");
db.empresas.createIndex({ "estabelecimentos.endereco.uf": 1 });

print("Criando indice por municipio...");
db.empresas.createIndex({ "estabelecimentos.endereco.municipio.codigo": 1 });

print("Criando indice por CNAE principal...");
db.empresas.createIndex({ "estabelecimentos.cnae_principal.codigo": 1 });

print("Criando indice por situacao cadastral...");
db.empresas.createIndex({ "estabelecimentos.situacao_cadastral.codigo": 1 });

print("Criando indice por CNPJ completo do estabelecimento...");
db.empresas.createIndex({ "estabelecimentos.cnpj": 1 });

print("Criando indice por nome do socio...");
db.empresas.createIndex({ "socios.nome": 1 });

print("Criando indice composto UF + situacao...");
db.empresas.createIndex({
    "estabelecimentos.endereco.uf": 1,
    "estabelecimentos.situacao_cadastral.codigo": 1
});

print("");
print("Indices criados com sucesso!");
print("");
print("Total de documentos: " + db.empresas.countDocuments());
print("");
print("Indices existentes:");
db.empresas.getIndexes().forEach(function(idx) {
    print("  - " + idx.name);
});
