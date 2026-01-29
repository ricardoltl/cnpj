FROM postgres:16-alpine

# Argumentos de build para inicialização do PostgreSQL
ARG POSTGRES_DB=cnpj_db
ARG POSTGRES_USER=cnpj
ARG POSTGRES_PASSWORD=cnpj123
ARG PGDATA=/var/lib/postgresql/data/pgdata

# Instalar dependências básicas
RUN apk add --no-cache \
    python3 \
    bash \
    curl \
    gcc \
    musl-dev \
    python3-dev \
    git

# Criar diretórios e definir workdir
RUN mkdir -p /app/scripts /app/config /app/data_incoming /app/logs
WORKDIR /app

# Instalar UV
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    mv /root/.local/bin/uv /usr/local/bin/uv && \
    mv /root/.local/bin/uvx /usr/local/bin/uvx

# Copiar arquivos do projeto
COPY scripts/ /app/scripts/
COPY config/ /app/config/
COPY pyproject.toml /app/

# Instalar dependências usando UV
RUN uv pip install --system --break-system-packages -r pyproject.toml

# Extrair dados da Receita Federal
COPY data_incoming/ /app/data_incoming/
#RUN cd /app && python3 scripts/cnpj_extractor.py

# Inicializar PostgreSQL, criar banco, importar dados e finalizar (tudo em um único RUN)
RUN mkdir -p "$PGDATA" && \
    chown -R postgres:postgres "$PGDATA" && \
    chmod 700 "$PGDATA" && \
    echo "$POSTGRES_PASSWORD" > /tmp/pwfile && \
    su-exec postgres initdb -D "$PGDATA" --username="$POSTGRES_USER" --pwfile=/tmp/pwfile && \
    rm /tmp/pwfile && \
    echo "host all all 127.0.0.1/32 trust" >> "$PGDATA/pg_hba.conf" && \
    echo "local all all trust" >> "$PGDATA/pg_hba.conf" && \
    su-exec postgres pg_ctl -D "$PGDATA" -o "-c listen_addresses='127.0.0.1' -c port=5432" -w start && \
    su-exec postgres psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres -c "CREATE DATABASE $POSTGRES_DB;" && \
    sleep 5 && \
    cd /app && POSTGRES_HOST=localhost python3 scripts/cnpj_importer.py && \
    su-exec postgres pg_ctl -D "$PGDATA" -m fast -w stop && \
    rm -rf /app/data_incoming/* /app/logs/*

# Expor porta do PostgreSQL
EXPOSE 5432

# Voltar ao entrypoint original do postgres
CMD ["postgres"]
