-- =============================================================================
-- stg_teams — Alinhamento 1:1 com raw.teams
-- Camada: Staging | Materialização: view
-- =============================================================================
select
    team_id,
    trim(team_name)                                         as team_name,
    coalesce(trim(country), 'UNKNOWN')                      as country,
    cast(founded_year as integer)                           as founded_year,
    cast(updated_at as timestamp)                           as updated_at
from {{ source('raw', 'teams') }}
where team_id is not null
  and team_name is not null
