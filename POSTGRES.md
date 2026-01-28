# PostgreSQL - Guia de Uso

## 🐘 Subir a instância do PostgreSQL

```bash
# Subir o PostgreSQL e pgAdmin
docker compose up -d

# Verificar se está rodando
docker compose ps

# Ver logs
docker compose logs -f postgres
```

## 📊 Importar dados dos arquivos .parquet

```bash
# Certifique-se de que os arquivos .parquet estão em data_outgoing/
# Execute o script de importação
uv run python scripts/import_to_postgres.py
```

O script irá:
1. Criar todas as tabelas necessárias
2. Importar dados dos arquivos .parquet em ordem (lookups primeiro)
3. Criar índices otimizados (incluindo índices para busca fuzzy)
4. Usar batches para evitar consumo excessivo de memória

## 🔌 Conexões

### PostgreSQL (psql, DBeaver, etc)
- **Host:** localhost
- **Porta:** 5432
- **Database:** cnpj_db
- **User:** cnpj
- **Password:** cnpj123

### pgAdmin (Interface Web)
- **URL:** http://localhost:8080
- **Email:** admin@admin.com
- **Password:** admin123

#### Configurar servidor no pgAdmin:
1. Acesse http://localhost:8080
2. Click direito em "Servers" → "Register" → "Server"
3. **General Tab:**
   - Name: CNPJ
4. **Connection Tab:**
   - Host: postgres (nome do container)
   - Port: 5432
   - Database: cnpj_db
   - Username: cnpj
   - Password: cnpj123

## 🔍 Exemplos de Consultas

### Busca Fuzzy por Razão Social
```sql
-- Busca empresas com nome similar
SELECT 
    e.razao_social, 
    est.telefone1, 
    est.correio_eletronico,
    similarity(e.razao_social, 'ACADEMIA SMART FIT') as similaridade
FROM empresas e
JOIN estabelecimentos est ON e.cnpj_basico = est.cnpj_basico
WHERE e.razao_social % 'ACADEMIA SMART FIT'
ORDER BY similarity(e.razao_social, 'ACADEMIA SMART FIT') DESC
LIMIT 10;
```

### Buscar empresas por UF e CNAE
```sql
SELECT 
    e.cnpj_basico || est.cnpj_ordem || est.cnpj_dv as cnpj_completo,
    e.razao_social,
    est.nome_fantasia,
    m.nome_municipio,
    est.uf,
    est.cnae_fiscal_principal
FROM empresas e
JOIN estabelecimentos est ON e.cnpj_basico = est.cnpj_basico
LEFT JOIN municipios m ON est.municipio = m.codigo_municipio
WHERE est.uf = 'SP'
  AND est.cnae_fiscal_principal = '8599604'
  AND est.situacao_cadastral = '02'
LIMIT 100;
```

### Contar empresas por UF
```sql
SELECT 
    est.uf,
    COUNT(*) as total_empresas
FROM estabelecimentos est
WHERE est.situacao_cadastral = '02'  -- Ativas
  AND est.identificador_matriz_filial = '1'  -- Apenas matrizes
GROUP BY est.uf
ORDER BY total_empresas DESC;
```

### Buscar sócios de uma empresa
```sql
SELECT 
    s.nome_do_socio,
    s.cnpj_ou_cpf_do_socio,
    q.descricao_qualificacao,
    s.data_de_entrada_sociedade
FROM socios s
LEFT JOIN qualificacoes q ON s.qualificacao_do_socio = q.codigo_qualificacao
WHERE s.cnpj_basico = '00000000'
ORDER BY s.data_de_entrada_sociedade;
```

## 🛠️ Comandos Úteis

```bash
# Parar os containers
docker compose down

# Parar e remover volumes (apaga todos os dados!)
docker compose down -v

# Reiniciar apenas o PostgreSQL
docker compose restart postgres

# Entrar no container do PostgreSQL
docker exec -it cnpj-postgres psql -U cnpj -d cnpj_db

# Backup do banco
docker exec cnpj-postgres pg_dump -U cnpj cnpj_db > backup.sql

# Restaurar backup
cat backup.sql | docker exec -i cnpj-postgres psql -U cnpj -d cnpj_db
```

## 📈 Performance

O script usa:
- **COPY** para importação rápida (muito mais rápido que INSERT)
- **Batches** de 50.000 registros para evitar consumo excessivo de RAM
- **Índices otimizados** incluindo:
  - Índices GIN para busca fuzzy (pg_trgm)
  - Índices B-tree para buscas exatas
  - Índices compostos para queries frequentes

## 🔒 Segurança

⚠️ **IMPORTANTE:** Este setup é para desenvolvimento local. Em produção:
- Altere as senhas padrão
- Configure SSL/TLS
- Ajuste as permissões de rede
- Configure backups automáticos
