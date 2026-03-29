-- =============================================================================
-- stg_matches — Alinhamento 1:1 com raw.matches
-- Camada: Staging | Materialização: view
-- Responsabilidade: limpeza, cast explícito, renomeação para snake_case
-- =============================================================================
select
    match_id,
    cast(match_date as date)                                as match_date,
    home_team_id,
    away_team_id,
    cast(home_goals as integer)                             as home_goals,
    cast(away_goals as integer)                             as away_goals,
    coalesce(competition_id, 'UNKNOWN')                     as competition_id,
    home_goals + away_goals                                 as total_goals,
    case
        when home_goals > away_goals then 'HOME_WIN'
        when home_goals < away_goals then 'AWAY_WIN'
        else 'DRAW'
    end                                                     as match_result,
    cast(loaded_at as timestamp)                            as loaded_at
from {{ source('raw', 'matches') }}
where match_id is not null
  and match_date is not null
