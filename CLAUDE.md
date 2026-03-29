# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Platform Overview

A containerized data engineering platform using **dbt + Airflow + Spark + Superset + PostgreSQL** connected via a shared Docker bridge network (`dataplatform_net`). The use case is sports analytics (Brazilian football matches/teams).

## Common Commands

### Starting and Stopping the Platform

```bash
bash scripts/start_all.sh    # Full platform startup (ordered, with health checks)
bash scripts/stop_all.sh     # Full platform shutdown (reverse order)
bash scripts/init_network.sh # Create Docker bridge network (run once)
```

### dbt Commands (run inside dbt container)

```bash
# Execute inside the dbt container
docker exec dbt dbt deps                              # Install packages
docker exec dbt dbt build                             # Run + test all models
docker exec dbt dbt run --select staging              # Run specific layer
docker exec dbt dbt run --select +fct_match_predictions  # Run with upstream
docker exec dbt dbt test                              # Run all tests
docker exec dbt dbt test --select stg_matches         # Test a single model
docker exec dbt dbt run --select dbt_project/models/marts/fct_match_predictions.sql # Single model
docker exec dbt dbt snapshot                          # Run SCD Type 2 snapshots
docker exec dbt dbt seed                              # Load seed CSV data
docker exec dbt dbt source freshness                  # Check source data freshness
docker exec dbt dbt docs generate && dbt docs serve   # Regenerate docs
```

### Docker Service Management

```bash
# Start individual services
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

- **Staging**: 1:1 with sources, minimal transforms, views
- **Intermediate**: Reusable business logic as ephemeral CTEs — uses custom `rolling_avg()` macro with 5-game window
- **Marts**: Analytics-ready `dim_` and `fct_` tables; `fct_match_predictions` is **incremental**

### Airflow DAG (`dbt_pipeline`)

Daily at 6 AM: `dbt_deps → dbt_source_freshness → dbt_build_staging → dbt_build_intermediate → dbt_build_marts → dbt_test_all → dbt_docs_generate`

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
- `MCP/dbt_advanced_guidelines.md` — Engineering standards and patterns for this project
- `.env` — All credentials and environment variables (not committed)

## Testing Strategy

- **Generic tests**: `not_null`, `unique`, `relationships`, `accepted_values`, `dbt_expectations` range checks on model columns
- **Singular tests**: Custom SQL in `dbt_project/tests/` for business rules
- **Unit tests**: Native dbt 1.9 unit tests in `dbt_project/unit_tests/` covering `match_result` logic and `total_goals` calculation
- **Snapshots**: SCD Type 2 on `team_roster` via `dbt snapshot`

## dbt Conventions

- Surrogate keys use `dbt_utils.generate_surrogate_key()`
- Naming: `stg_` for staging, `int_` for intermediate, `fct_`/`dim_` for marts, `snp_` for snapshots
- Post-hook `ANALYZE` runs on all mart tables for query planner optimization
- `fct_match_predictions` uses incremental materialization — be careful when modifying its schema (may need `--full-refresh`)
