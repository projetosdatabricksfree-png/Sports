-- =============================================================================
-- stg_standings — Staging de classificações por liga e temporada
-- Camada: Staging | Materialização: view
-- Responsabilidade: cast explícito, rank anterior calculado via LAG
-- Grain: uma linha por time por liga por temporada por rodada
-- =============================================================================
with source as (
    select
        standing_id,
        league_id,
        league_name,
        season,
        team_id,
        team_name,
        cast(rank as integer)                              as rank,
        cast(points as integer)                            as points,
        cast(played as integer)                            as played,
        cast(win as integer)                               as win,
        cast(draw as integer)                              as draw,
        cast(lose as integer)                              as lose,
        cast(goals_for as integer)                         as goals_for,
        cast(goals_against as integer)                     as goals_against,
        cast(goals_for as integer)
            - cast(goals_against as integer)               as goal_difference,
        coalesce(form, '')                                 as form,
        status,
        description,
        cast(updated_at as timestamp)                      as updated_at,
        loaded_at
    from {{ source('football_raw', 'standings') }}
    where team_id is not null
      and league_id is not null
      and season is not null
),

with_prev_rank as (
    select
        *,
        lag(rank) over (
            partition by team_id, league_id, season
            order by updated_at
        )                                                  as prev_rank,
        rank - lag(rank) over (
            partition by team_id, league_id, season
            order by updated_at
        )                                                  as rank_change
    from source
)

select * from with_prev_rank
