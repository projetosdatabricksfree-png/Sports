#!/bin/bash
# =============================================================================
# Inicia toda a plataforma de dados na ordem correta
# Ordem: rede → postgres → airflow → spark → superset → dbeaver → dbt
# =============================================================================
set -e

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE_DIR"

echo "============================================================"
echo "  DATA PLATFORM — INICIALIZANDO"
echo "============================================================"

# 1. Rede Docker
echo "[1/7] Criando rede Docker..."
bash scripts/init_network.sh

# 2. PostgreSQL (base de tudo)
echo "[2/7] Iniciando PostgreSQL..."
docker compose -f docker/postgres/docker-compose.yml --env-file .env up -d
echo "      Aguardando PostgreSQL ficar saudável..."
until docker exec postgres pg_isready -U admin -d dataplatform &>/dev/null; do
    sleep 2
done
echo "      [OK] PostgreSQL pronto."

# 3. Apache Airflow
echo "[3/7] Iniciando Airflow (init + webserver + scheduler)..."
docker compose -f docker/airflow/docker-compose.yml --env-file .env up airflow-init
docker compose -f docker/airflow/docker-compose.yml --env-file .env up -d airflow-webserver airflow-scheduler

# 4. Apache Spark
echo "[4/7] Iniciando Spark..."
docker compose -f docker/spark/docker-compose.yml --env-file .env up -d

# 5. Apache Superset
echo "[5/7] Iniciando Superset..."
docker compose -f docker/superset/docker-compose.yml --env-file .env up -d

# 6. CloudBeaver (DBeaver Web)
echo "[6/7] Iniciando CloudBeaver..."
docker compose -f docker/dbeaver/docker-compose.yml --env-file .env up -d

# 7. dbt
echo "[7/7] Iniciando dbt..."
docker compose -f docker/dbt/docker-compose.yml --env-file .env up -d

echo ""
echo "============================================================"
echo "  PLATAFORMA INICIADA — ACESSOS"
echo "============================================================"
echo "  Airflow    : http://localhost:8080   (admin / admin)"
echo "  Spark UI   : http://localhost:8081"
echo "  Superset   : http://localhost:8088   (admin / admin)"
echo "  CloudBeaver: http://localhost:8978   (admin / admin)"
echo "  dbt docs   : http://localhost:8083"
echo "  PostgreSQL : localhost:5432          (admin / admin)"
echo "============================================================"
