#!/bin/bash
# =============================================================================
# Para toda a plataforma de dados (ordem reversa)
# =============================================================================
BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE_DIR"

echo "Parando a plataforma..."

docker compose -f docker/dbt/docker-compose.yml       down
docker compose -f docker/dbeaver/docker-compose.yml   down
docker compose -f docker/superset/docker-compose.yml  down
docker compose -f docker/spark/docker-compose.yml     down
docker compose -f docker/airflow/docker-compose.yml   down
docker compose -f docker/postgres/docker-compose.yml  down

echo "[OK] Plataforma parada."
