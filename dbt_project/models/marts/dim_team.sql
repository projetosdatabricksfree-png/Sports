-- =============================================================================
-- dim_team — Dimensão de times
-- Camada: Marts | Materialização: table
-- =============================================================================
select
    {{ dbt_utils.generate_surrogate_key(['team_id']) }}     as team_sk,
    team_id,
    team_name,
    country,
    founded_year,
    updated_at
from {{ ref('stg_teams') }}
