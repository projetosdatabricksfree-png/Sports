-- =============================================================================
-- int_head_to_head — Histórico de confrontos diretos entre times
-- Camada: Intermediate | Materialização: ephemeral
-- Responsabilidade: estatísticas H2H dos últimos 5 confrontos entre cada par
-- Grain: uma linha por par de times (team_a_id, team_b_id) ordenado
-- =============================================================================

-- Normaliza os confrontos: o par sempre aparece na mesma ordem (menor id primeiro)
with fixtures_normalized as (
    select
        fixture_id,
        match_date,
        league_id,
        season,
        home_team_id,
        away_team_id,
        home_goals,
        away_goals,
        total_goals,
        match_result,
        -- Normaliza o par para que menor id seja sempre team_a
        least(home_team_id, away_team_id)     as team_a_id,
        greatest(home_team_id, away_team_id)  as team_b_id
    from {{ ref('stg_fixtures') }}
),

-- Ranking dos confrontos por par, do mais recente para o mais antigo
ranked_h2h as (
    select
        *,
        row_number() over (
            partition by team_a_id, team_b_id
            order by match_date desc
        )                                     as h2h_rank
    from fixtures_normalized
),

-- Apenas os últimos 5 confrontos entre cada par
last_5_h2h as (
    select *
    from ranked_h2h
    where h2h_rank <= 5
),

-- Agrega estatísticas H2H
h2h_stats as (
    select
        team_a_id,
        team_b_id,
        count(*)                                                     as h2h_total_matches,
        -- Vitórias do mandante original (home_team_id)
        sum(case when match_result = 'HOME_WIN' then 1 else 0 end)  as h2h_home_wins,
        sum(case when match_result = 'DRAW'     then 1 else 0 end)  as h2h_draws,
        sum(case when match_result = 'AWAY_WIN' then 1 else 0 end)  as h2h_away_wins,
        round(avg(total_goals)::numeric, 3)                         as h2h_avg_goals,
        round(avg(home_goals)::numeric, 3)                          as h2h_avg_home_goals,
        round(avg(away_goals)::numeric, 3)                          as h2h_avg_away_goals,
        max(match_date)                                              as h2h_last_match_date,
        min(match_date)                                              as h2h_first_match_date
    from last_5_h2h
    group by team_a_id, team_b_id
)

select
    team_a_id,
    team_b_id,
    h2h_total_matches,
    h2h_home_wins,
    h2h_draws,
    h2h_away_wins,
    h2h_avg_goals,
    h2h_avg_home_goals,
    h2h_avg_away_goals,
    h2h_last_match_date,
    h2h_first_match_date,
    -- Taxa de vitória do time A quando joga em casa contra time B
    round(
        (h2h_home_wins::numeric / nullif(h2h_total_matches, 0)), 3
    )                                                                as h2h_home_win_rate,
    -- Taxa de empate
    round(
        (h2h_draws::numeric / nullif(h2h_total_matches, 0)), 3
    )                                                                as h2h_draw_rate
from h2h_stats
