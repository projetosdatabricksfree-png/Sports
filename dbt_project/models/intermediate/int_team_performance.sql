-- =============================================================================
-- int_team_performance — Performance agregada por time e partida
-- Camada: Intermediate | Materialização: ephemeral (CTE reutilizável)
-- Combina stg_matches + stg_teams, calcula rolling form index
-- =============================================================================
with home_matches as (
    select
        home_team_id                                        as team_id,
        match_date,
        match_id,
        home_goals                                          as goals_scored,
        away_goals                                          as goals_conceded,
        case when match_result = 'HOME_WIN' then 3
             when match_result = 'DRAW'     then 1
             else 0
        end                                                 as points
    from {{ ref('stg_matches') }}
),

away_matches as (
    select
        away_team_id                                        as team_id,
        match_date,
        match_id,
        away_goals                                          as goals_scored,
        home_goals                                          as goals_conceded,
        case when match_result = 'AWAY_WIN' then 3
             when match_result = 'DRAW'     then 1
             else 0
        end                                                 as points
    from {{ ref('stg_matches') }}
),

all_matches as (
    select * from home_matches
    union all
    select * from away_matches
),

with_rolling as (
    select
        am.team_id,
        am.match_id,
        am.match_date,
        am.goals_scored,
        am.goals_conceded,
        am.goals_scored - am.goals_conceded                 as goal_diff,
        am.points,
        -- Rolling form index: média de pontos nos últimos 5 jogos
        {{ rolling_avg('am.points',      'am.team_id', 'am.match_date', 5) }} as rolling_points_avg,
        -- Rolling gols marcados nos últimos 5 jogos
        {{ rolling_avg('am.goals_scored','am.team_id', 'am.match_date', 5) }} as rolling_goals_avg,
        t.team_name
    from all_matches am
    left join {{ ref('stg_teams') }} t
        on am.team_id = t.team_id
)

select * from with_rolling
