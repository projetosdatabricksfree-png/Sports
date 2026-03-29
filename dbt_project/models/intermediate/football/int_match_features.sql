-- =============================================================================
-- int_match_features — Features completas por partida para ML
-- Camada: Intermediate | Materialização: ephemeral
-- Responsabilidade: combinar fixture + form de ambos os times + H2H + odds
--   produzindo um vetor de features pronto para treinamento
-- Grain: uma linha por partida finalizada
-- =============================================================================

-- Form do time mandante no momento do jogo (perspectiva agregada, qualquer side)
with home_form as (
    select
        fixture_id,
        team_id                              as home_team_id,
        avg_points_l5                        as home_avg_points_l5,
        avg_goals_scored_l5                  as home_avg_goals_scored_l5,
        avg_goals_conceded_l5                as home_avg_goals_conceded_l5,
        win_rate_l5                          as home_win_rate_l5,
        avg_points_l10                       as home_avg_points_l10,
        avg_goals_scored_l10                 as home_avg_goals_scored_l10,
        avg_goals_conceded_l10               as home_avg_goals_conceded_l10,
        win_rate_l10                         as home_win_rate_l10
    from {{ ref('int_team_form') }}
),

-- Form do time visitante no momento do jogo
away_form as (
    select
        fixture_id,
        team_id                              as away_team_id,
        avg_points_l5                        as away_avg_points_l5,
        avg_goals_scored_l5                  as away_avg_goals_scored_l5,
        avg_goals_conceded_l5                as away_avg_goals_conceded_l5,
        win_rate_l5                          as away_win_rate_l5,
        avg_points_l10                       as away_avg_points_l10,
        avg_goals_scored_l10                 as away_avg_goals_scored_l10,
        avg_goals_conceded_l10               as away_avg_goals_conceded_l10,
        win_rate_l10                         as away_win_rate_l10
    from {{ ref('int_team_form') }}
),

-- H2H normalizado: recupera stats para o par (home_team_id, away_team_id)
h2h as (
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
        h2h_home_win_rate,
        h2h_draw_rate
    from {{ ref('int_head_to_head') }}
),

-- Odds implícitas: média entre bookmakers por fixture (mercado 1X2)
avg_odds as (
    select
        fixture_id,
        round(avg(implied_prob_home)::numeric, 4) as odds_implied_prob_home,
        round(avg(implied_prob_draw)::numeric, 4) as odds_implied_prob_draw,
        round(avg(implied_prob_away)::numeric, 4) as odds_implied_prob_away,
        round(avg(odd_home)::numeric, 4)           as avg_odd_home,
        round(avg(odd_draw)::numeric, 4)           as avg_odd_draw,
        round(avg(odd_away)::numeric, 4)           as avg_odd_away,
        count(distinct bookmaker_name)             as bookmaker_count
    from {{ ref('stg_odds') }}
    where market_name = 'Match Winner'
    group by fixture_id
),

-- Vantagem histórica do mandante: % de vitórias em casa no mesmo venue
home_venue_advantage as (
    select
        venue_name,
        home_team_id,
        count(*)                                                       as venue_total_matches,
        round(
            avg(case when match_result = 'HOME_WIN' then 1.0 else 0.0 end)::numeric, 4
        )                                                              as venue_home_win_rate,
        round(avg(home_goals)::numeric, 4)                            as venue_avg_home_goals,
        round(avg(away_goals)::numeric, 4)                            as venue_avg_away_goals
    from {{ ref('stg_fixtures') }}
    where venue_name is not null
    group by venue_name, home_team_id
),

-- Montagem final do vetor de features
final as (
    select
        -- Identificadores
        f.fixture_id,
        f.league_id,
        f.league_name,
        f.season,
        f.round,
        f.match_date,
        f.match_day,
        f.match_year,
        f.match_month,
        f.day_of_week,

        -- Times
        f.home_team_id,
        f.home_team_name,
        f.away_team_id,
        f.away_team_name,

        -- Target (variável alvo)
        f.match_result,
        f.home_goals,
        f.away_goals,
        f.total_goals,
        f.home_goals_ht,
        f.away_goals_ht,
        f.venue_name,

        -- Form do mandante (l5)
        coalesce(hf.home_avg_points_l5,      0) as home_avg_points_l5,
        coalesce(hf.home_avg_goals_scored_l5, 0) as home_avg_goals_scored_l5,
        coalesce(hf.home_avg_goals_conceded_l5, 0) as home_avg_goals_conceded_l5,
        coalesce(hf.home_win_rate_l5,        0) as home_win_rate_l5,

        -- Form do mandante (l10)
        coalesce(hf.home_avg_points_l10,     0) as home_avg_points_l10,
        coalesce(hf.home_avg_goals_scored_l10, 0) as home_avg_goals_scored_l10,
        coalesce(hf.home_avg_goals_conceded_l10, 0) as home_avg_goals_conceded_l10,
        coalesce(hf.home_win_rate_l10,       0) as home_win_rate_l10,

        -- Form do visitante (l5)
        coalesce(af.away_avg_points_l5,      0) as away_avg_points_l5,
        coalesce(af.away_avg_goals_scored_l5, 0) as away_avg_goals_scored_l5,
        coalesce(af.away_avg_goals_conceded_l5, 0) as away_avg_goals_conceded_l5,
        coalesce(af.away_win_rate_l5,        0) as away_win_rate_l5,

        -- Form do visitante (l10)
        coalesce(af.away_avg_points_l10,     0) as away_avg_points_l10,
        coalesce(af.away_avg_goals_scored_l10, 0) as away_avg_goals_scored_l10,
        coalesce(af.away_avg_goals_conceded_l10, 0) as away_avg_goals_conceded_l10,
        coalesce(af.away_win_rate_l10,       0) as away_win_rate_l10,

        -- Diferencial de forma (feature derivada)
        coalesce(hf.home_avg_points_l5, 0) - coalesce(af.away_avg_points_l5, 0)
            as form_diff_points_l5,
        coalesce(hf.home_avg_points_l10, 0) - coalesce(af.away_avg_points_l10, 0)
            as form_diff_points_l10,

        -- H2H stats (baseado nos últimos 5 confrontos)
        coalesce(h2h.h2h_total_matches,   0) as h2h_total_matches,
        coalesce(h2h.h2h_home_wins,       0) as h2h_home_wins,
        coalesce(h2h.h2h_draws,           0) as h2h_draws,
        coalesce(h2h.h2h_away_wins,       0) as h2h_away_wins,
        coalesce(h2h.h2h_avg_goals,       0) as h2h_avg_goals,
        coalesce(h2h.h2h_home_win_rate,   0) as h2h_home_win_rate,
        coalesce(h2h.h2h_draw_rate,       0) as h2h_draw_rate,

        -- Vantagem histórica do venue
        coalesce(va.venue_total_matches,   0) as venue_total_matches,
        coalesce(va.venue_home_win_rate,   0) as venue_home_win_rate,
        coalesce(va.venue_avg_home_goals,  0) as venue_avg_home_goals,
        coalesce(va.venue_avg_away_goals,  0) as venue_avg_away_goals,

        -- Odds implícitas (quando disponíveis)
        ao.odds_implied_prob_home,
        ao.odds_implied_prob_draw,
        ao.odds_implied_prob_away,
        ao.avg_odd_home,
        ao.avg_odd_draw,
        ao.avg_odd_away,
        ao.bookmaker_count,

        f.loaded_at

    from {{ ref('stg_fixtures') }} f

    -- Form do mandante: join pela partida e pelo time
    left join home_form hf
        on f.fixture_id = hf.fixture_id
       and f.home_team_id = hf.home_team_id

    -- Form do visitante: join pela partida e pelo time
    left join away_form af
        on f.fixture_id = af.fixture_id
       and f.away_team_id = af.away_team_id

    -- H2H: join pelo par normalizado
    left join h2h
        on least(f.home_team_id, f.away_team_id)    = h2h.team_a_id
       and greatest(f.home_team_id, f.away_team_id) = h2h.team_b_id

    -- Odds médias
    left join avg_odds ao
        on f.fixture_id = ao.fixture_id

    -- Vantagem histórica do venue
    left join home_venue_advantage va
        on f.venue_name    = va.venue_name
       and f.home_team_id  = va.home_team_id
)

select * from final
