# =============================================================================
# dbt Core 1.9.2 — Container para execução de transformações e docs
# Base: python:3.12-slim (mínima, segura, mantida)
# =============================================================================
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    libpq-dev \
    gcc \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Instalação do dbt-core + adaptador PostgreSQL + pacotes de qualidade
RUN pip install --no-cache-dir \
    dbt-core==1.9.1 \
    dbt-postgres==1.9.1 \
    dbt-utils==1.3.0 \
    dbt-expectations==0.10.4 \
    elementary-data==0.16.3

WORKDIR /dbt_project

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
