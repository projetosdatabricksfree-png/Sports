-- =============================================================================
-- dim_league — Dimensão de ligas de futebol
-- Camada: Marts | Materialização: table
-- Grain: uma linha por liga (league_id)
-- =============================================================================
{{
  config(
    materialized = 'table'
  )
}}

with leagues_from_fixtures as (
    select distinct
        league_id,
        league_name
    from {{ ref('stg_fixtures') }}
    where league_id is not null
),

leagues_from_standings as (
    select distinct
        league_id,
        league_name
    from {{ ref('stg_standings') }}
    where league_id is not null
),

-- Consolida informações de ligas de múltiplas fontes
all_leagues as (
    select league_id, league_name from leagues_from_fixtures
    union
    select league_id, league_name from leagues_from_standings
),

-- Temporadas disponíveis por liga
seasons_per_league as (
    select
        league_id,
        min(season)                                              as first_season,
        max(season)                                              as last_season,
        count(distinct season)                                   as total_seasons,
        array_agg(distinct season order by season)               as available_seasons
    from {{ ref('stg_fixtures') }}
    group by league_id
),

-- Estatísticas gerais por liga
league_stats as (
    select
        league_id,
        count(*)                                                 as total_matches,
        round(avg(total_goals)::numeric, 3)                     as avg_goals_per_match,
        round(
            avg(case when match_result = 'HOME_WIN' then 1.0 else 0.0 end)::numeric, 4
        )                                                        as home_win_rate,
        round(
            avg(case when match_result = 'DRAW' then 1.0 else 0.0 end)::numeric, 4
        )                                                        as draw_rate,
        round(
            avg(case when match_result = 'AWAY_WIN' then 1.0 else 0.0 end)::numeric, 4
        )                                                        as away_win_rate
    from {{ ref('stg_fixtures') }}
    group by league_id
),

final as (
    select
        al.league_id,
        al.league_name,
        -- Classificação simples: ligas com "Cup" ou "Copa" no nome são copas
        case
            when lower(al.league_name) like '%cup%'
              or lower(al.league_name) like '%copa%'
              or lower(al.league_name) like '%coupe%'
              or lower(al.league_name) like '%pokal%'
              or lower(al.league_name) like '%taça%'
            then 'CUP'
            when lower(al.league_name) like '%champions%'
              or lower(al.league_name) like '%europa%'
              or lower(al.league_name) like '%conference%'
              or lower(al.league_name) like '%world cup%'
              or lower(al.league_name) like '%nations%'
            then 'INTERNATIONAL'
            else 'DOMESTIC_LEAGUE'
        end                                                      as league_type,
        coalesce(sp.first_season, null)                          as first_season,
        coalesce(sp.last_season,  null)                          as last_season,
        coalesce(sp.total_seasons, 0)                            as total_seasons,
        sp.available_seasons,
        coalesce(ls.total_matches, 0)                            as total_matches,
        ls.avg_goals_per_match,
        ls.home_win_rate,
        ls.draw_rate,
        ls.away_win_rate
    from all_leagues al
    left join seasons_per_league sp on al.league_id = sp.league_id
    left join league_stats       ls on al.league_id = ls.league_id
)

select * from final
