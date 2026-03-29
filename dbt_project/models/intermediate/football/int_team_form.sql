-- =============================================================================
-- int_team_form — Forma recente dos times (janela de 5 e 10 jogos)
-- Camada: Intermediate | Materialização: ephemeral
-- Responsabilidade: calcular rolling form por time, perspectiva home e away unificada
-- Grain: uma linha por time por partida (registro de participação)
-- =============================================================================

-- Perspectiva mandante: como o time jogou em casa
with home_perspective as (
    select
        home_team_id                                            as team_id,
        league_id,
        season,
        match_date,
        fixture_id,
        home_goals                                              as goals_scored,
        away_goals                                              as goals_conceded,
        case
            when match_result = 'HOME_WIN' then 3
            when match_result = 'DRAW'     then 1
            else 0
        end                                                     as points,
        case when match_result = 'HOME_WIN' then 1 else 0 end  as is_win,
        'HOME'                                                  as perspective
    from {{ ref('stg_fixtures') }}
),

-- Perspectiva visitante: como o time jogou fora de casa
away_perspective as (
    select
        away_team_id                                            as team_id,
        league_id,
        season,
        match_date,
        fixture_id,
        away_goals                                              as goals_scored,
        home_goals                                              as goals_conceded,
        case
            when match_result = 'AWAY_WIN' then 3
            when match_result = 'DRAW'     then 1
            else 0
        end                                                     as points,
        case when match_result = 'AWAY_WIN' then 1 else 0 end  as is_win,
        'AWAY'                                                  as perspective
    from {{ ref('stg_fixtures') }}
),

-- União de todas as participações
all_appearances as (
    select * from home_perspective
    union all
    select * from away_perspective
),

-- Cálculo das janelas deslizantes (5 e 10 jogos)
with_rolling as (
    select
        team_id,
        league_id,
        season,
        match_date,
        fixture_id,
        goals_scored,
        goals_conceded,
        points,
        is_win,
        perspective,

        -- Janela de 5 jogos
        {{ rolling_avg('points',          'team_id, league_id, season', 'match_date', 5) }}
            as avg_points_l5,
        {{ rolling_avg('goals_scored',    'team_id, league_id, season', 'match_date', 5) }}
            as avg_goals_scored_l5,
        {{ rolling_avg('goals_conceded',  'team_id, league_id, season', 'match_date', 5) }}
            as avg_goals_conceded_l5,
        {{ rolling_avg('is_win',          'team_id, league_id, season', 'match_date', 5) }}
            as win_rate_l5,

        -- Janela de 10 jogos
        {{ rolling_avg('points',          'team_id, league_id, season', 'match_date', 10) }}
            as avg_points_l10,
        {{ rolling_avg('goals_scored',    'team_id, league_id, season', 'match_date', 10) }}
            as avg_goals_scored_l10,
        {{ rolling_avg('goals_conceded',  'team_id, league_id, season', 'match_date', 10) }}
            as avg_goals_conceded_l10,
        {{ rolling_avg('is_win',          'team_id, league_id, season', 'match_date', 10) }}
            as win_rate_l10

    from all_appearances
)

select * from with_rolling
