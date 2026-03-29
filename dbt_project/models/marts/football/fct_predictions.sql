-- =============================================================================
-- fct_predictions — Previsões de resultados de partidas
-- Camada: Marts | Materialização: incremental (merge por fixture_id)
-- Grain: uma linha por partida prevista
-- Alimentada pelo pipeline Python de ML (Poisson + Gradient Boosting + Híbrido)
-- =============================================================================
{{
  config(
    materialized     = 'incremental',
    unique_key       = 'fixture_id',
    on_schema_change = 'sync_all_columns'
  )
}}

with predictions_raw as (
    -- Esta tabela é populada externamente pelo pipeline Python de ML.
    -- O modelo dbt apenas organiza, valida e enriquece as previsões gravadas
    -- em raw.predictions pelo script Python.
    select
        fixture_id,
        -- Probabilidades do modelo de Poisson
        cast(poisson_home_prob as numeric(6, 4))  as poisson_home_prob,
        cast(poisson_draw_prob as numeric(6, 4))  as poisson_draw_prob,
        cast(poisson_away_prob as numeric(6, 4))  as poisson_away_prob,
        -- Probabilidades do Gradient Boosting
        cast(gb_home_prob      as numeric(6, 4))  as gb_home_prob,
        cast(gb_draw_prob      as numeric(6, 4))  as gb_draw_prob,
        cast(gb_away_prob      as numeric(6, 4))  as gb_away_prob,
        -- Probabilidades híbridas
        cast(hybrid_home_prob  as numeric(6, 4))  as hybrid_home_prob,
        cast(hybrid_draw_prob  as numeric(6, 4))  as hybrid_draw_prob,
        cast(hybrid_away_prob  as numeric(6, 4))  as hybrid_away_prob,
        -- Gols esperados
        cast(expected_home_goals as numeric(6, 3)) as expected_home_goals,
        cast(expected_away_goals as numeric(6, 3)) as expected_away_goals,
        -- Score de confiança do modelo
        cast(confidence_score as numeric(6, 4))  as confidence_score,
        model_version,
        cast(predicted_at as timestamp)           as predicted_at
    from {{ source('football_raw', 'fixtures') }}
    -- Nota: substituir pela tabela raw.predictions quando disponível.
    -- Esta CTE é um placeholder que será preenchida pelo pipeline Python.
    where false  -- garante que o modelo compila sem dados reais ainda
),

fixtures as (
    select
        fixture_id,
        league_id,
        match_date,
        home_team_id,
        away_team_id,
        match_result                              as actual_result,
        home_goals                                as actual_home_goals,
        away_goals                                as actual_away_goals
    from {{ ref('stg_fixtures') }}
),

final as (
    select
        p.fixture_id,
        f.league_id,
        f.match_date,
        f.home_team_id,
        f.away_team_id,

        -- Modelo Poisson
        p.poisson_home_prob,
        p.poisson_draw_prob,
        p.poisson_away_prob,

        -- Gradient Boosting
        p.gb_home_prob,
        p.gb_draw_prob,
        p.gb_away_prob,

        -- Híbrido (ensemble final)
        p.hybrid_home_prob,
        p.hybrid_draw_prob,
        p.hybrid_away_prob,

        -- Resultado previsto (baseado no híbrido — maior probabilidade)
        case
            when p.hybrid_home_prob >= p.hybrid_draw_prob
             and p.hybrid_home_prob >= p.hybrid_away_prob then 'HOME_WIN'
            when p.hybrid_away_prob >= p.hybrid_draw_prob
             and p.hybrid_away_prob >  p.hybrid_home_prob then 'AWAY_WIN'
            else 'DRAW'
        end                                       as predicted_result,

        -- Score de confiança (probabilidade da classe prevista)
        p.confidence_score,

        -- Gols esperados
        p.expected_home_goals,
        p.expected_away_goals,

        -- Resultado real (quando disponível — partida já ocorreu)
        f.actual_result,
        f.actual_home_goals,
        f.actual_away_goals,

        -- Flag de acerto
        case
            when f.actual_result is not null
             and case
                    when p.hybrid_home_prob >= p.hybrid_draw_prob
                     and p.hybrid_home_prob >= p.hybrid_away_prob then 'HOME_WIN'
                    when p.hybrid_away_prob >= p.hybrid_draw_prob
                     and p.hybrid_away_prob >  p.hybrid_home_prob then 'AWAY_WIN'
                    else 'DRAW'
                 end = f.actual_result
            then true
            when f.actual_result is not null then false
            else null
        end                                       as is_correct,

        -- Metadados do modelo
        p.model_version,
        p.predicted_at,
        current_timestamp                         as dbt_loaded_at
    from predictions_raw p
    inner join fixtures f on p.fixture_id = f.fixture_id
)

select * from final

{% if is_incremental() %}
  where predicted_at > (select max(predicted_at) from {{ this }})
{% endif %}
