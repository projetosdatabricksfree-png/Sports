-- =============================================================================
-- fct_match_predictions — Fato de partidas enriquecido com form index
-- Camada: Marts | Materialização: incremental (append/merge por match_date)
-- Grain: uma linha por partida
-- =============================================================================
{{
  config(
    materialized     = 'incremental',
    unique_key       = 'match_id',
    on_schema_change = 'sync_all_columns'
  )
}}

with home_form as (
    select
        match_id,
        team_id                                             as home_team_id,
        rolling_points_avg                                  as home_rolling_form,
        rolling_goals_avg                                   as home_rolling_goals,
        team_name                                           as home_team_name
    from {{ ref('int_team_performance') }}
),

away_form as (
    select
        match_id,
        team_id                                             as away_team_id,
        rolling_points_avg                                  as away_rolling_form,
        rolling_goals_avg                                   as away_rolling_goals,
        team_name                                           as away_team_name
    from {{ ref('int_team_performance') }}
),

final as (
    select
        m.match_id,
        m.match_date,
        m.competition_id,
        m.home_team_id,
        h.home_team_name,
        m.away_team_id,
        a.away_team_name,
        m.home_goals,
        m.away_goals,
        m.total_goals,
        m.match_result,
        coalesce(h.home_rolling_form, 0)                    as home_form_index,
        coalesce(a.away_rolling_form, 0)                    as away_form_index,
        coalesce(h.home_rolling_goals, 0)                   as home_rolling_goals,
        coalesce(a.away_rolling_goals, 0)                   as away_rolling_goals,
        -- Índice simples de vantagem do mandante baseado em form
        coalesce(h.home_rolling_form, 0)
            - coalesce(a.away_rolling_form, 0)              as home_advantage_index,
        m.loaded_at
    from {{ ref('stg_matches') }} m
    left join home_form h on m.match_id = h.match_id and m.home_team_id = h.home_team_id
    left join away_form a on m.match_id = a.match_id and m.away_team_id = a.away_team_id
)

select * from final

{% if is_incremental() %}
  where match_date > (select max(match_date) from {{ this }})
{% endif %}
