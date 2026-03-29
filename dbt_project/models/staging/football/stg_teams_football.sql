-- =============================================================================
-- stg_teams_football — Staging de times de futebol
-- Camada: Staging | Materialização: view
-- Responsabilidade: cast explícito, padronização de campos, limpeza
-- Grain: uma linha por time
-- =============================================================================
select
    team_id,
    team_name,
    coalesce(team_code, '')                  as team_code,
    country,
    coalesce(founded, null)                  as founded_year,
    cast(is_national as boolean)             as is_national,
    logo_url,
    venue_id,
    venue_name,
    venue_city,
    venue_country,
    cast(venue_capacity as integer)          as venue_capacity,
    cast(updated_at as timestamp)            as updated_at
from {{ source('football_raw', 'teams_football') }}
where team_id is not null
  and team_name is not null
