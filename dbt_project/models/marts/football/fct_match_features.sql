-- =============================================================================
-- fct_match_features — Features para ML materializadas como tabela
-- Camada: Marts | Materialização: table
-- Grain: uma linha por partida finalizada
-- Usada pelo pipeline Python de ML como input de treinamento e avaliação
-- =============================================================================
{{
  config(
    materialized  = 'table',
    unique_key    = 'fixture_id'
  )
}}

with features as (
    select * from {{ ref('int_match_features') }}
),

-- Enriquece com surrogate keys das dimensões
final as (
    select
        -- Identificadores e chaves
        f.fixture_id,
        ht.team_sk                                               as home_team_sk,
        at_.team_sk                                              as away_team_sk,
        dl.league_type,

        -- Contexto da partida
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

        -- Variável alvo
        f.match_result,
        -- Encoding numérico do target (para modelos que exigem)
        case
            when f.match_result = 'HOME_WIN' then 0
            when f.match_result = 'DRAW'     then 1
            when f.match_result = 'AWAY_WIN' then 2
        end                                                      as match_result_encoded,
        f.home_goals,
        f.away_goals,
        f.total_goals,
        f.home_goals_ht,
        f.away_goals_ht,

        -- Features de forma — mandante
        f.home_avg_points_l5,
        f.home_avg_goals_scored_l5,
        f.home_avg_goals_conceded_l5,
        f.home_win_rate_l5,
        f.home_avg_points_l10,
        f.home_avg_goals_scored_l10,
        f.home_avg_goals_conceded_l10,
        f.home_win_rate_l10,

        -- Features de forma — visitante
        f.away_avg_points_l5,
        f.away_avg_goals_scored_l5,
        f.away_avg_goals_conceded_l5,
        f.away_win_rate_l5,
        f.away_avg_points_l10,
        f.away_avg_goals_scored_l10,
        f.away_avg_goals_conceded_l10,
        f.away_win_rate_l10,

        -- Diferenciais de forma
        f.form_diff_points_l5,
        f.form_diff_points_l10,
        -- Diferencial de gols esperados (atacante vs defesa adversária)
        f.home_avg_goals_scored_l5 - f.away_avg_goals_conceded_l5
            as home_attack_vs_away_defense_l5,
        f.away_avg_goals_scored_l5 - f.home_avg_goals_conceded_l5
            as away_attack_vs_home_defense_l5,

        -- H2H
        f.h2h_total_matches,
        f.h2h_home_wins,
        f.h2h_draws,
        f.h2h_away_wins,
        f.h2h_avg_goals,
        f.h2h_home_win_rate,
        f.h2h_draw_rate,

        -- Vantagem do venue
        f.venue_name,
        f.venue_total_matches,
        f.venue_home_win_rate,
        f.venue_avg_home_goals,
        f.venue_avg_away_goals,

        -- Odds / probabilidades de mercado
        f.odds_implied_prob_home,
        f.odds_implied_prob_draw,
        f.odds_implied_prob_away,
        f.avg_odd_home,
        f.avg_odd_draw,
        f.avg_odd_away,
        f.bookmaker_count,
        -- Flag: odds disponíveis para esta partida
        case when f.bookmaker_count > 0 then true else false end as has_odds,

        -- Metadados
        f.loaded_at,
        current_timestamp                                        as dbt_loaded_at
    from features f
    left join {{ ref('dim_team_football') }} ht
        on f.home_team_id = ht.team_id
    left join {{ ref('dim_team_football') }} at_
        on f.away_team_id = at_.team_id
    left join {{ ref('dim_league') }} dl
        on f.league_id = dl.league_id
)

select * from final
