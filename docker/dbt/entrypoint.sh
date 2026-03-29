#!/bin/bash
# =============================================================================
# dbt — Entrypoint: instala deps e mantém container ativo para exec remoto
# =============================================================================
set -e

echo ">>> dbt version: $(dbt --version)"
echo ">>> Instalando packages..."
cd /dbt_project && dbt deps --profiles-dir /dbt_project 2>/dev/null || true
echo ">>> dbt container pronto."

# Mantém o container ativo para docker exec
exec tail -f /dev/null
