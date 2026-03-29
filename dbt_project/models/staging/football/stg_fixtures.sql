-- =============================================================================
-- stg_fixtures — Staging de partidas (fixtures) de futebol
-- Camada: Staging | Materialização: view
-- Responsabilidade: cast explícito, derivações de data, filtro de jogos finalizados
-- Grain: uma linha por partida finalizada
-- =============================================================================
select
    fixture_id,
    league_id,
    league_name,
    season,
    round,
    cast(match_date as timestamp)            as match_date,
    cast(match_date::date as date)           as match_day,
    extract(year  from match_date::date)     as match_year,
    extract(month from match_date::date)     as match_month,
    extract(dow   from match_date::date)     as day_of_week,
    status,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    coalesce(home_goals, 0)                  as home_goals,
    coalesce(away_goals, 0)                  as away_goals,
    coalesce(home_goals_ht, 0)               as home_goals_ht,
    coalesce(away_goals_ht, 0)               as away_goals_ht,
    coalesce(home_goals, 0)
        + coalesce(away_goals, 0)            as total_goals,
    case
        when home_goals > away_goals then 'HOME_WIN'
        when away_goals > home_goals then 'AWAY_WIN'
        when home_goals = away_goals then 'DRAW'
    end                                      as match_result,
    venue_name,
    referee,
    loaded_at
from {{ source('football_raw', 'fixtures') }}
where status in ('FT', 'AET', 'PEN')  -- apenas jogos finalizados
