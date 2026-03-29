-- =============================================================================
-- dim_team_football — Dimensão de times de futebol com surrogate key
-- Camada: Marts | Materialização: table
-- Grain: uma linha por time (team_id)
-- =============================================================================
{{
  config(
    materialized = 'table'
  )
}}

with teams as (
    select * from {{ ref('stg_teams_football') }}
),

-- Enriquece com estatísticas históricas de participação
match_stats as (
    select
        home_team_id                                             as team_id,
        count(distinct league_id)                               as leagues_played,
        count(distinct season)                                  as seasons_played,
        count(*)                                                as total_home_matches
    from {{ ref('stg_fixtures') }}
    group by home_team_id

    union all

    select
        away_team_id                                             as team_id,
        count(distinct league_id)                               as leagues_played,
        count(distinct season)                                  as seasons_played,
        count(*)                                                as total_away_matches
    from {{ ref('stg_fixtures') }}
    group by away_team_id
),

aggregated_stats as (
    select
        team_id,
        max(leagues_played)                                      as leagues_played,
        max(seasons_played)                                      as seasons_played,
        sum(total_home_matches)                                  as total_matches
    from match_stats
    group by team_id
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['t.team_id']) }}    as team_sk,
        t.team_id,
        t.team_name,
        t.team_code,
        t.country,
        t.founded_year,
        t.is_national,
        t.logo_url,
        t.venue_id,
        t.venue_name,
        t.venue_city,
        t.venue_country,
        t.venue_capacity,
        coalesce(s.leagues_played,  0)                           as leagues_played,
        coalesce(s.seasons_played,  0)                           as seasons_played,
        coalesce(s.total_matches,   0)                           as total_matches,
        t.updated_at,
        current_timestamp                                        as dbt_loaded_at
    from teams t
    left join aggregated_stats s on t.team_id = s.team_id
)

select * from final
