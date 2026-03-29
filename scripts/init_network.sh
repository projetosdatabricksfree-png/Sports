#!/bin/bash
# =============================================================================
# Cria a rede Docker compartilhada entre todos os serviços
# =============================================================================
NETWORK="dataplatform_net"

if docker network inspect "$NETWORK" &>/dev/null; then
    echo "[OK] Rede '$NETWORK' já existe."
else
    docker network create --driver bridge "$NETWORK"
    echo "[CRIADA] Rede '$NETWORK' criada com sucesso."
fi
