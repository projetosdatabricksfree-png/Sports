#!/bin/bash
# =============================================================================
# Football Prediction Platform — Inicialização completa
# Ordem: rede → postgres → airflow → spark → superset → dbeaver → dbt → mlflow
# =============================================================================
set -e

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE_DIR"

echo "============================================================"
echo "  FOOTBALL PREDICTION PLATFORM — INICIALIZANDO"
echo "============================================================"

# 1. Rede Docker
echo "[1/8] Criando rede Docker..."
bash scripts/init_network.sh

# 2. PostgreSQL (base de tudo)
echo "[2/8] Iniciando PostgreSQL..."
docker compose -f docker/postgres/docker-compose.yml --env-file .env up -d
echo "      Aguardando PostgreSQL ficar saudável..."
until docker exec postgres pg_isready -U admin -d dataplatform &>/dev/null; do
    sleep 2
done
echo "      [OK] PostgreSQL pronto."

# 3. Apache Airflow
echo "[3/8] Iniciando Airflow (init + webserver + scheduler)..."
docker compose -f docker/airflow/docker-compose.yml --env-file .env up airflow-init
docker compose -f docker/airflow/docker-compose.yml --env-file .env up -d airflow-webserver airflow-scheduler

# 4. Apache Spark
echo "[4/8] Iniciando Spark..."
docker compose -f docker/spark/docker-compose.yml --env-file .env up -d

# 5. Apache Superset
echo "[5/8] Iniciando Superset..."
docker compose -f docker/superset/docker-compose.yml --env-file .env up -d

# 6. CloudBeaver (DBeaver Web)
echo "[6/8] Iniciando CloudBeaver..."
docker compose -f docker/dbeaver/docker-compose.yml --env-file .env up -d

# 7. dbt
echo "[7/8] Iniciando dbt..."
docker compose -f docker/dbt/docker-compose.yml --env-file .env up -d

# 8. MLflow (Model Registry + Tracking)
echo "[8/8] Iniciando MLflow..."
docker compose -f docker/mlflow/docker-compose.yml --env-file .env up -d
echo "      Aguardando MLflow..."
sleep 10
echo "      [OK] MLflow pronto."

echo ""
echo "============================================================"
echo "  FOOTBALL PREDICTION PLATFORM — ACESSOS"
echo "============================================================"
echo "  Airflow    : http://localhost:8080   (admin / admin)"
echo "  Spark UI   : http://localhost:8081"
echo "  dbt docs   : http://localhost:8083"
echo "  Superset   : http://localhost:8088   (admin / admin)"
echo "  MLflow     : http://localhost:5000"
echo "  CloudBeaver: http://localhost:8978   (admin / admin)"
echo "  PostgreSQL : localhost:5432          (admin / admin)"
echo "============================================================"
echo "  DAG principal: football_prediction_pipeline"
echo "  API Football : https://dashboard.api-football.com/"
echo "============================================================"
