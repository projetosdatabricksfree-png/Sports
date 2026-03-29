-- =============================================================================
-- fct_team_season_stats — Estatísticas agregadas por time na temporada
-- Camada: Marts | Materialização: table
-- Grain: uma linha por time
-- =============================================================================
select
    {{ dbt_utils.generate_surrogate_key(['team_id', 'season']) }} as stat_sk,
    team_id,
    team_name,
    extract(year from match_date)::integer                  as season,
    count(match_id)                                         as total_matches,
    sum(points)                                             as total_points,
    sum(goals_scored)                                       as total_goals_scored,
    sum(goals_conceded)                                     as total_goals_conceded,
    sum(goal_diff)                                          as total_goal_diff,
    sum(case when points = 3 then 1 else 0 end)             as wins,
    sum(case when points = 1 then 1 else 0 end)             as draws,
    sum(case when points = 0 then 1 else 0 end)             as losses,
    round(
        sum(case when points = 3 then 1 else 0 end)::numeric
        / nullif(count(match_id), 0), 3
    )                                                       as win_rate,
    round(avg(rolling_points_avg), 3)                       as avg_rolling_form,
    round(avg(rolling_goals_avg), 3)                        as avg_rolling_goals,
    current_timestamp                                       as updated_at
from {{ ref('int_team_performance') }}
group by team_id, team_name, extract(year from match_date)::integer
