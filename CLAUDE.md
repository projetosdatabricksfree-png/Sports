# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Platform Overview

A containerized data engineering platform using **dbt + Airflow + Spark + Superset + PostgreSQL** connected via a shared Docker bridge network (`dataplatform_net`). The use case is sports analytics (Brazilian football matches/teams).

---

## MCP Servers Configurados

Os MCPs estão definidos em `.claude/settings.local.json` e são ativados automaticamente no Claude Code. Seguem o modelo operacional definido em `MCP/dbt_advanced_guidelines.md`.

### Protocolo obrigatório antes de criar ou modificar qualquer modelo dbt

| Etapa | Ação | MCP |
|---|---|---|
| 1 | Inspecionar schemas e colunas existentes | `postgres` |
| 2 | Validar dados com queries ao vivo | `postgres` |
| 3 | Verificar o que mudou recentemente no arquivo | `git` |
| 4 | Executar comandos dbt | `filesystem` (logs) |
| 5 | Analisar falhas e SQL compilado | `filesystem` |
| 6 | Validar outputs pós-execução | `postgres` |

---

### `postgres` — Database MCP

**Pacote:** `@modelcontextprotocol/server-postgres`
**Conexão:** `postgresql://admin:admin@localhost:5432/dataplatform`

Usado em todas as etapas de desenvolvimento de modelos dbt.

| Ferramenta | Quando usar |
|---|---|
| Inspecionar `information_schema` | Antes de criar staging — confirmar colunas e tipos da raw |
| Query ad-hoc nas tabelas raw | Validar distribuição, nulos e cardinalidade da fonte |
| Query nas tabelas de staging/marts | Validar output após `dbt run` |
| Verificar chaves duplicadas | Antes de definir `unique_key` em modelo incremental |
| Conferir integridade referencial | Antes de definir `relationships` tests |

```sql
-- Exemplos de queries de validação via postgres MCP
select column_name, data_type from information_schema.columns
where table_schema = 'raw' and table_name = 'matches';

select match_result, count(*) from staging.stg_matches group by 1;
```

---

### `filesystem` — Filesystem MCP

**Pacote:** `@modelcontextprotocol/server-filesystem`
**Raiz:** `/home/diego/EngenhariaDeDados`

| Caminho | O que contém | Quando acessar |
|---|---|---|
| `dbt_project/logs/dbt.log` | Log completo da última execução dbt | Quando `dbt run` ou `dbt test` falhar |
| `dbt_project/target/compiled/` | SQL compilado de cada modelo | Para debugar erro de sintaxe ou lógica |
| `dbt_project/target/run/` | SQL executado com resultados | Para auditoria pós-execução |
| `dbt_project/models/` | Código-fonte dos modelos | Para leitura e edição |
| `docker/airflow/logs/` | Logs das DAGs do Airflow | Quando pipeline falhar no scheduler |

**Fluxo de debug com Filesystem MCP:**
```
1. Ler logs/dbt.log → identificar modelo e erro
2. Ler target/compiled/<model>.sql → ver SQL gerado
3. Executar SQL via postgres MCP → reproduzir erro
4. Corrigir modelo → re-executar com --select <model>
```

---

### `git` — Git MCP

**Pacote:** `@modelcontextprotocol/server-git`
**Repositório:** `/home/diego/EngenhariaDeDados`

| Ferramenta | Quando usar |
|---|---|
| `git log` via MCP | Ver histórico de alterações de um modelo antes de modificar |
| `git diff` via MCP | Comparar versão atual com anterior antes de fazer commit |
| `git blame` via MCP | Identificar quem e quando alterou determinada linha |

**Regra:** Sempre consultar o histórico git de um modelo antes de modificar lógica existente em `intermediate/` ou `marts/`.

---

### `github` — GitHub MCP

**Pacote:** `@modelcontextprotocol/server-github`
**Repositório:** `projetosdatabricksfree-png/Sports`
**Autenticação:** variável de ambiente `GITHUB_TOKEN`

| Ferramenta | Quando usar |
|---|---|
| Criar/listar issues | Registrar bugs ou novas features no repositório |
| Abrir Pull Requests | Propor merge de `feature/*` → `develop` → `main` |
| Revisar PRs abertos | Verificar status antes de fazer merge |
| Buscar código | Pesquisar implementações anteriores no histórico |

**Setup do token (execute uma vez):**
```bash
export GITHUB_TOKEN=seu_token_aqui
# Ou adicione ao ~/.bashrc para persistir
echo 'export GITHUB_TOKEN=seu_token_aqui' >> ~/.bashrc
```

---

### Mapa de MCPs por ferramenta da plataforma

| Ferramenta | MCP Principal | MCP Secundário | Caso de uso |
|---|---|---|---|
| **dbt** (modelos) | `postgres` | `filesystem` | Inspecionar schema → escrever modelo → validar output |
| **dbt** (debug) | `filesystem` | `postgres` | Ler log → ver SQL compilado → reproduzir erro |
| **dbt** (histórico) | `git` | — | Ver o que mudou antes de alterar modelo existente |
| **Airflow** (debug) | `filesystem` | — | Ler logs de DAGs com falha |
| **PostgreSQL** (exploração) | `postgres` | — | Queries ad-hoc em qualquer schema |
| **GitHub** (PR/issues) | `github` | `git` | Criar PR de feature branch para develop |
| **Geral** (versionamento) | `git` | `github` | Rastrear alterações e publicar no repositório |

---

## Common Commands

### Starting and Stopping the Platform

```bash
bash scripts/start_all.sh    # Full platform startup (ordered, with health checks)
bash scripts/stop_all.sh     # Full platform shutdown (reverse order)
bash scripts/init_network.sh # Create Docker bridge network (run once)
```

### dbt Commands (run inside dbt container)

```bash
docker exec dbt dbt deps                                  # Install packages
docker exec dbt dbt build                                 # Run + test all models
docker exec dbt dbt run --select staging                  # Run specific layer
docker exec dbt dbt run --select +fct_match_predictions   # Run with upstream
docker exec dbt dbt test                                  # Run all tests
docker exec dbt dbt test --select stg_matches             # Test a single model
docker exec dbt dbt snapshot                              # Run SCD Type 2 snapshots
docker exec dbt dbt seed                                  # Load seed CSV data
docker exec dbt dbt source freshness                      # Check source data freshness
docker exec dbt dbt docs generate                         # Regenerate docs
```

### Docker Service Management

```bash
docker compose -f docker/postgres/docker-compose.yml up -d
docker compose -f docker/airflow/docker-compose.yml up -d
docker compose -f docker/dbt/docker-compose.yml up -d
docker compose -f docker/spark/docker-compose.yml up -d
docker compose -f docker/superset/docker-compose.yml up -d
docker compose -f docker/dbeaver/docker-compose.yml up -d

# Logs
docker compose -f docker/airflow/docker-compose.yml logs -f airflow-scheduler
docker logs dbt -f
```

---

## Architecture

### Service Ports and Credentials (all use `admin/admin`)

| Service | URL | Purpose |
|---|---|---|
| Airflow | http://localhost:8080 | DAG orchestration |
| Spark Master | http://localhost:8081 | Distributed compute |
| dbt docs | http://localhost:8083 | Data lineage & docs |
| Superset | http://localhost:8088 | BI dashboards |
| CloudBeaver | http://localhost:8978 | Web DB browser |
| PostgreSQL | localhost:5432 | Central data store |

### Database Schemas

- `raw` — Source tables (`matches`, `teams`, `team_roster`), seeded by `docker/postgres/init/01_init.sql`
- `staging` — dbt views (`stg_matches`, `stg_teams`)
- `intermediate` — Ephemeral CTEs (`int_team_performance`)
- `marts` — Final analytics tables (`dim_team`, `fct_team_season_stats`, `fct_match_predictions`)
- `snapshots` — SCD Type 2 history (`snp_team_roster`)

### dbt Layered Architecture

```
raw (postgres source) → staging (views) → intermediate (ephemeral) → marts (tables)
```

- **Staging**: 1:1 com fontes, transformações mínimas, materializado como view
- **Intermediate**: Lógica de negócio como CTEs efêmeras — usa macro `rolling_avg()` com janela de 5 jogos
- **Marts**: Tabelas analíticas finais `dim_` e `fct_`; `fct_match_predictions` é **incremental**

### Airflow DAG (`dbt_pipeline`)

Daily at 6 AM: `dbt_deps → dbt_source_freshness → dbt_build_staging → dbt_build_intermediate → dbt_build_marts → dbt_test_all → dbt_docs_generate`

---

## Key Files

- `dbt_project/dbt_project.yml` — Layer materializations, variable definitions (`rolling_window: 5`, freshness hours)
- `dbt_project/profiles.yml` — Connection profiles for `dev` and `prod` targets
- `dbt_project/packages.yml` — dbt-utils 1.3.0, dbt-expectations 0.10.4, elementary-data 0.16.3
- `dbt_project/macros/` — `rolling_avg()` custom window macro
- `dbt_project/tests/` — Singular SQL tests (goals non-negative, win rate bounds)
- `dbt_project/unit_tests/` — Native dbt 1.9 unit tests for `stg_matches` logic
- `dbt_project/snapshots/` — `snp_team_roster` SCD Type 2
- `dbt_project/seeds/competition_mapping.csv` — 4 competition records loaded to `raw` schema
- `dbt_project/exposures/match_analytics.yml` — Superset dashboard + Airflow DAG exposures
- `docker/airflow/dags/dbt_pipeline.py` — Airflow DAG definition
- `MCP/dbt_advanced_guidelines.md` — Engineering standards, MCP operating model and patterns
- `.claude/settings.local.json` — MCP servers configuration
- `.env` — All credentials and environment variables (not committed)

---

## Testing Strategy

- **Generic tests**: `not_null`, `unique`, `relationships`, `accepted_values`, `dbt_expectations` range checks on model columns
- **Singular tests**: Custom SQL in `dbt_project/tests/` for business rules
- **Unit tests**: Native dbt 1.9 unit tests in `dbt_project/unit_tests/` covering `match_result` logic and `total_goals` calculation
- **Snapshots**: SCD Type 2 on `team_roster` via `dbt snapshot`

---

## dbt Conventions

- Surrogate keys use `dbt_utils.generate_surrogate_key()`
- Naming: `stg_` staging, `int_` intermediate, `fct_`/`dim_` marts, `snp_` snapshots
- Post-hook `ANALYZE` runs on all mart tables for query planner optimization
- `fct_match_predictions` uses incremental materialization — schema changes may need `--full-refresh`
- Never reference raw tables directly with `from raw.schema.table` — always use `source()` macro
