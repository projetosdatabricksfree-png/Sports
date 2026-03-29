-- =============================================================================
-- fct_standings — Tabela de classificação atual por liga e temporada
-- Camada: Marts | Materialização: table
-- Post-hook: ANALYZE (configurado globalmente no dbt_project.yml para marts)
-- Grain: uma linha por time por liga por temporada (snapshot mais recente)
-- =============================================================================
{{
  config(
    materialized = 'table'
  )
}}

with latest_snapshot as (
    -- Pega o snapshot mais recente por time/liga/temporada
    select
        standing_id,
        league_id,
        league_name,
        season,
        team_id,
        team_name,
        rank,
        prev_rank,
        rank_change,
        points,
        played,
        win,
        draw,
        lose,
        goals_for,
        goals_against,
        goal_difference,
        form,
        status,
        description,
        updated_at,
        row_number() over (
            partition by team_id, league_id, season
            order by updated_at desc
        )                                                        as rn
    from {{ ref('stg_standings') }}
),

current_standings as (
    select *
    from latest_snapshot
    where rn = 1
),

-- Enriquece com surrogate key do time
enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['cs.league_id', 'cs.season', 'cs.team_id']) }}
            as standing_sk,
        cs.league_id,
        cs.league_name,
        cs.season,
        cs.team_id,
        cs.team_name,
        -- Dimensão de time (para join com dim_team_football)
        dt.team_sk,
        cs.rank,
        cs.prev_rank,
        coalesce(cs.rank_change, 0)                             as rank_change,
        -- Sinal de movimento: subiu, desceu ou manteve
        case
            when cs.rank_change < 0 then 'UP'
            when cs.rank_change > 0 then 'DOWN'
            else 'SAME'
        end                                                      as rank_movement,
        cs.points,
        cs.played,
        cs.win,
        cs.draw,
        cs.lose,
        cs.goals_for,
        cs.goals_against,
        cs.goal_difference,
        -- Médias por jogo
        round((cs.goals_for::numeric    / nullif(cs.played, 0)), 3) as avg_goals_scored,
        round((cs.goals_against::numeric / nullif(cs.played, 0)), 3) as avg_goals_conceded,
        round((cs.points::numeric        / nullif(cs.played, 0)), 3) as avg_points_per_game,
        round((cs.win::numeric           / nullif(cs.played, 0)), 4) as win_rate,
        round((cs.draw::numeric          / nullif(cs.played, 0)), 4) as draw_rate,
        round((cs.lose::numeric          / nullif(cs.played, 0)), 4) as loss_rate,
        cs.form,
        cs.status,
        cs.description,
        cs.updated_at,
        -- Liga
        dl.league_type,
        dl.avg_goals_per_match                                  as league_avg_goals,
        dl.home_win_rate                                        as league_home_win_rate
    from current_standings cs
    left join {{ ref('dim_team_football') }} dt
        on cs.team_id = dt.team_id
    left join {{ ref('dim_league') }} dl
        on cs.league_id = dl.league_id
)

select * from enriched
