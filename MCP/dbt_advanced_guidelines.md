# Advanced Data Engineering Guidelines with dbt

> **MCP · Open Source · Databricks · Analytics Engineering Excellence**

---

## Table of Contents

1. [Objective](#objective)
2. [Core Principle](#core-principle)
3. [AI + MCP Operating Model](#ai--mcp-operating-model)
4. [Project Structure](#project-structure)
5. [Layered Modeling Strategy](#layered-modeling-strategy)
6. [Naming Conventions](#naming-conventions)
7. [Model Materialization](#model-materialization)
8. [Testing Strategy](#testing-strategy)
9. [Documentation Standards](#documentation-standards)
10. [Performance Optimization](#performance-optimization)
11. [Data Freshness & Snapshots](#data-freshness--snapshots)
12. [MCP Integration with dbt](#mcp-integration-with-dbt)
13. [Standard Workflow](#standard-workflow)
14. [Anti-Patterns](#anti-patterns)
15. [Advanced Best Practices](#advanced-best-practices)
16. [Real Use Case — Sports Analytics](#real-use-case--sports-analytics)

---

## Objective

Establish a **production-grade framework** for data engineering and analytics engineering using:

| Pillar | Technology |
|--------|-----------|
| Transformation Layer | **dbt** (data build tool) |
| AI Integration | **MCP** (Model Context Protocol) |
| Compute & Storage | **Databricks Lakehouse** |
| Infrastructure | **Open Source Stack** |

### Guiding Outcomes

- **Data reliability** — consistent, trustworthy outputs at every layer
- **Modular transformations** — composable, independently testable models
- **Reproducibility** — deterministic builds across environments
- **Observability** — full lineage, testing, and run-level visibility
- **Performance optimization** — efficient query design and materialization

---

## Core Principle

> *"Transformations must be declarative, testable, and versioned."*

**dbt is the single source of truth for all transformations.** No business logic lives in the BI layer, application layer, or ad-hoc scripts. Every transformation is a model, every model has a test, and every test is version-controlled.

---

## AI + MCP Operating Model

When operating in AI-assisted mode, the agent **must** execute the following sequence before writing or modifying any model:

| Step | Action | Tool |
|------|--------|------|
| 1 | Inspect existing schemas | **Database MCP** |
| 2 | Validate assumptions with live queries | **Database MCP** |
| 3 | Execute dbt commands | **Process MCP** |
| 4 | Analyze run failures and errors | **Logs MCP** |
| 5 | Validate model outputs post-run | **Database MCP** |

> **Constraint:** No model is created or modified without completing steps 1 and 2 first.

---

## Project Structure

The following directory layout is **mandatory** across all dbt projects:

```
dbt_project/
│
├── models/
│   ├── staging/          # Raw source alignment (stg_)
│   ├── intermediate/     # Business logic composition (int_)
│   └── marts/            # Analytics-ready outputs (fct_ / dim_)
│
├── seeds/                # Static reference datasets
├── snapshots/            # SCD Type 2 change tracking
├── tests/                # Custom singular and generic tests
├── macros/               # Reusable Jinja logic
├── analyses/             # Ad-hoc analytical SQL (non-materialized)
└── dbt_project.yml       # Project configuration
```

---

## Layered Modeling Strategy

### Layer 1 — Staging (`stg_`)

**Purpose:** Align raw source data to a clean, standardized contract.

| Rule | Detail |
|------|--------|
| Source mapping | 1:1 with source tables — one model per source object |
| Joins | **Not allowed** |
| Column naming | Rename to business-friendly, snake_case names |
| Data types | Explicit casting on every column |
| Logic | Light transformations only (nullif, coalesce, trim) |

```sql
-- models/staging/stg_matches.sql
select
    match_id,
    cast(match_date as date)           as match_date,
    home_team_id,
    away_team_id,
    cast(home_goals as int)            as home_goals,
    cast(away_goals as int)            as away_goals,
    loaded_at
from {{ source('raw', 'matches') }}
```

---

### Layer 2 — Intermediate (`int_`)

**Purpose:** Combine datasets and encode business logic.

| Rule | Detail |
|------|--------|
| Joins | Allowed — combine staging models |
| Aggregations | Avoid heavy aggregations; prefer marts layer |
| Complexity | Break complex logic into smaller intermediate models |
| References | Use `ref()` exclusively — never raw table references |

```sql
-- models/intermediate/int_team_performance.sql
select
    m.match_date,
    t.team_name,
    sum(m.goals_scored)                as total_goals,
    count(m.match_id)                  as total_matches
from {{ ref('stg_matches') }}          as m
left join {{ ref('stg_teams') }}       as t
    on m.team_id = t.team_id
group by 1, 2
```

---

### Layer 3 — Marts (`fct_` / `dim_`)

**Purpose:** Deliver business-ready, analytics-grade datasets.

| Model Type | Prefix | Content |
|------------|--------|---------|
| Fact tables | `fct_` | Metrics, events, measurable activities |
| Dimension tables | `dim_` | Descriptive attributes, hierarchies, entities |

| Rule | Detail |
|------|--------|
| Aggregation | Fully aggregated or analytics-ready |
| Grain | Clearly defined and documented |
| Consumers | BI tools, ML pipelines, data products |

---

## Naming Conventions

All model names follow this **mandatory** pattern:

| Layer | Pattern | Example |
|-------|---------|---------|
| Staging | `stg_<source>_<entity>` | `stg_api_matches` |
| Intermediate | `int_<domain>_<logic>` | `int_team_rolling_form` |
| Fact | `fct_<business_process>` | `fct_match_predictions` |
| Dimension | `dim_<entity>` | `dim_team` |
| Snapshot | `snp_<entity>` | `snp_team_roster` |
| Seed | `<entity>_mapping` | `competition_mapping` |

---

## Model Materialization

Materialization is chosen based on model type and data volume:

| Materialization | Use Case | Applied To |
|-----------------|----------|-----------|
| `view` | Low-cost, always fresh | Staging layer |
| `table` | Analytics-ready, queried frequently | Marts layer |
| `incremental` | Large datasets, append/merge patterns | High-volume facts |
| `ephemeral` | Intermediate CTEs, not queried directly | Optional — use sparingly |

### Incremental Model Requirements

When using incremental materialization, the following are **non-negotiable**:

```yaml
# dbt_project.yml
models:
  my_project:
    marts:
      +materialized: table
    staging:
      +materialized: view
```

```sql
-- models/marts/fct_match_predictions.sql
{{
  config(
    materialized = 'incremental',
    unique_key   = 'match_id',
    on_schema_change = 'sync_all_columns'
  )
}}

select * from {{ ref('int_team_performance') }}

{% if is_incremental() %}
  where match_date > (select max(match_date) from {{ this }})
{% endif %}
```

**Rules for incremental models:**
- Always define a `unique_key`
- Handle late-arriving data explicitly
- Avoid `--full-refresh` in production unless planned

---

## Testing Strategy

### Built-in Generic Tests

Every model column that carries a logical constraint **must** be tested:

```yaml
# models/marts/schema.yml
models:
  - name: fct_matches
    columns:
      - name: match_id
        tests:
          - not_null
          - unique

      - name: team_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_team')
              field: team_id

      - name: match_date
        tests:
          - not_null
```

### Custom Tests

Extend built-in tests with domain-specific assertions:

| Category | Examples |
|----------|---------|
| Business rules | Goals cannot be negative; match must have two teams |
| Anomaly detection | Row count deviation > 20% vs. prior run |
| Threshold validation | Win rate must be between 0 and 1 |
| Freshness | Source must have loaded within the past 24 hours |

```sql
-- tests/assert_goals_non_negative.sql
select match_id
from {{ ref('fct_matches') }}
where home_goals < 0
   or away_goals < 0
```

### Test Coverage Standard

| Layer | Minimum Coverage |
|-------|-----------------|
| Staging | `not_null` + `unique` on all PKs |
| Intermediate | Referential integrity on all FKs |
| Marts | Business rule tests + anomaly detection |

---

## Documentation Standards

### Requirement

Every model **must** include model-level and column-level documentation. Undocumented models are non-compliant.

```yaml
# models/marts/schema.yml
models:
  - name: fct_matches
    description: >
      Fact table containing match-level statistics and outcomes.
      Grain: one row per match. Refreshed daily via incremental strategy.

    columns:
      - name: match_id
        description: "Unique identifier for each match. Source: API Futebol."

      - name: match_date
        description: "Date the match was played (UTC)."

      - name: home_goals
        description: "Total goals scored by the home team."

      - name: away_goals
        description: "Total goals scored by the away team."

      - name: expected_goals_home
        description: "xG metric for the home team, calculated in int_team_performance."
```

### Lineage via Exposures

Define BI-layer consumers to enable full lineage visibility:

```yaml
# exposures/match_analytics.yml
exposures:
  - name: match_dashboard
    type: dashboard
    maturity: high
    url: https://your-bi-tool.com/dashboards/matches
    description: "Match performance dashboard for the analytics team."
    depends_on:
      - ref('fct_matches')
      - ref('dim_team')
    owner:
      name: Diego Oliveira
      email: diego@company.com
```

---

## Performance Optimization

### dbt + Databricks Delta Lake

| Optimization | When to Apply |
|---|---|
| `OPTIMIZE` | Post-load on large Delta tables |
| `ZORDER BY` | High-cardinality filter columns (e.g., `team_id`, `match_date`) |
| Partitioning | Date columns and high-cardinality dimensions |
| Delta caching | Frequently queried mart tables |

```sql
-- Post-hook example in dbt_project.yml
models:
  my_project:
    marts:
      +post-hook:
        - "OPTIMIZE {{ this }} ZORDER BY (match_date, team_id)"
```

### Query Design Rules

| Rule | Rationale |
|------|-----------|
| Filter early | Push `WHERE` clauses as close to the source as possible |
| Avoid CTE overuse | Materialize intermediate steps when reused across models |
| Minimize joins | Denormalize at the mart layer rather than joining in BI |
| Use window functions | Prefer over self-joins for rolling calculations |

---

## Data Freshness & Snapshots

### Source Freshness

```yaml
# models/sources.yml
sources:
  - name: raw
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    loaded_at_field: loaded_at
    tables:
      - name: matches
```

### Snapshots — SCD Type 2

Used to track **Slowly Changing Dimensions** (roster changes, team data, classifications):

```sql
-- snapshots/snp_team_roster.sql
{% snapshot snp_team_roster %}

{{
  config(
    target_schema = 'snapshots',
    unique_key    = 'player_id',
    strategy      = 'timestamp',
    updated_at    = 'updated_at'
  )
}}

select * from {{ source('raw', 'team_roster') }}

{% endsnapshot %}
```

---

## MCP Integration with dbt

### Database MCP — Schema Validation

```
# Before creating any model:
1. Query information_schema to validate source columns exist
2. Confirm data types match expectations
3. Sample the source for nulls, cardinality, and distribution
4. Verify output of previous model if extending a pipeline
```

### Process MCP — Command Execution

```bash
# Standard execution sequence
dbt deps                        # Resolve package dependencies
dbt source freshness            # Validate source data age
dbt build --select staging.*    # Build + test staging layer
dbt build --select marts.*      # Build + test marts layer
dbt docs generate               # Generate documentation site
```

### Logs MCP — Failure Debugging

```
# On dbt run failure:
1. Retrieve logs/dbt.log from the run directory
2. Identify failing model and error message
3. Query the database to reproduce the error
4. Inspect compiled SQL in target/compiled/
5. Fix model, re-run with --select <model_name>
```

---

## Standard Workflow

The following sequence is the **standard deployment workflow** and must not be altered without architectural approval:

```
┌──────────────────────────────────────────────────────────────────┐
│                    dbt DEPLOYMENT WORKFLOW                        │
├────┬─────────────────────────────────────────────────────────────┤
│  1 │  Ingest raw data into Bronze / Raw layer                    │
│  2 │  Inspect schemas via Database MCP                           │
│  3 │  Create or update staging models (stg_)                     │
│  4 │  Build intermediate business logic (int_)                   │
│  5 │  Create or update mart models (fct_ / dim_)                 │
│  6 │  Run dbt test — validate all layers                         │
│  7 │  Validate outputs via Database MCP ad-hoc queries           │
│  8 │  Generate and review dbt docs                               │
│  9 │  Open Pull Request → CI pipeline runs dbt build + test      │
│ 10 │  Merge to main → deploy to production                       │
└────┴─────────────────────────────────────────────────────────────┘
```

---

## Anti-Patterns

The following patterns are **explicitly prohibited**:

| ❌ Anti-Pattern | Why It Is Harmful |
|---|---|
| Business logic in staging layer | Violates single-responsibility; breaks layering contract |
| Massive SQL in a single model | Untestable, unmaintainable, impossible to debug |
| Models without tests | No guarantee of correctness; silent failures in production |
| Hardcoded values in SQL | Breaks portability across environments; use `var()` or seeds |
| Ignoring incremental strategy | Full table scans on large datasets; cost explosion |
| BI tool as transformation layer | Logic is invisible, untested, and unversioned |
| Raw table references with `from raw.schema.table` | Bypasses `source()` — breaks lineage and freshness checks |
| Circular dependencies between models | Breaks DAG resolution; dbt will error at compile time |

---

## Advanced Best Practices

| Practice | Implementation |
|---|---|
| **Macros for reusable logic** | Define date spines, type casting, business rules as Jinja macros |
| **Seeds for static datasets** | Country codes, competition mappings, product hierarchies |
| **Exposures for BI lineage** | Register all dashboards and ML consumers in `exposures/` |
| **Version control everything** | All models, tests, docs, and configs live in Git |
| **Separate dev / prod environments** | Use `profiles.yml` targets; never run dev builds against prod data |
| **CI/CD enforcement** | Require `dbt build` to pass before any PR merge |
| **Package management** | Use `packages.yml` with pinned versions (`dbt-utils`, `dbt-expectations`) |
| **Environment variables** | Never hardcode credentials; use `env_var()` macro |

---

## Real Use Case — Sports Analytics

### Data Flow

```
                    ┌─────────────────────────────┐
                    │     API Ingestion (Bronze)    │
                    │  matches · odds · lineups     │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │      Staging Layer           │
                    │  stg_api_matches             │
                    │  stg_api_odds                │
                    │  stg_api_teams               │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │    Intermediate Layer        │
                    │  int_team_performance        │
                    │  int_match_odds_enriched     │
                    └──────────────┬──────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
 ┌────────────▼──────┐  ┌──────────▼──────┐  ┌─────────▼────────┐
 │   fct_match_      │  │  fct_team_       │  │   dim_team       │
 │   predictions     │  │  season_stats    │  │   dim_competition│
 └───────────────────┘  └─────────────────┘  └──────────────────┘
```

### Key Metrics

| Metric | Model | Logic |
|---|---|---|
| Win Rate | `fct_team_season_stats` | `wins / total_matches` |
| Expected Goals (xG) | `int_team_performance` | Derived from shot data |
| Rolling Form Index | `int_team_performance` | 5-match rolling avg of points |
| Prediction Confidence | `fct_match_predictions` | ML model output (0–1 probability) |

### Example — Rolling Form Macro

```sql
-- macros/rolling_avg.sql
{% macro rolling_avg(column, partition_by, order_by, window=5) %}
    avg({{ column }}) over (
        partition by {{ partition_by }}
        order by {{ order_by }}
        rows between {{ window - 1 }} preceding and current row
    )
{% endmacro %}

-- Usage in model
{{ rolling_avg('goals_scored', 'team_id', 'match_date', 5) }} as rolling_goals_avg
```

---

## Enforcement

> *"dbt is not just a tool — it is the **contract layer** of your data platform."*

This document carries the following status:

| Attribute | Value |
|-----------|-------|
| **Classification** | Mandatory Guideline |
| **Scope** | All dbt projects across the data platform |
| **Role** | Architecture Standard + AI Decision Framework |
| **Compliance** | Required for all model creation and modification |
| **Review Cycle** | Quarterly or upon major dbt version upgrade |

**All implementations must comply with this document in full.** Exceptions require explicit architectural approval and must be documented with justification in the model's `description` field.

---

*Advanced Data Engineering Guidelines with dbt — v1.0*
*Analytics Engineering · Databricks Lakehouse · MCP Integration*
