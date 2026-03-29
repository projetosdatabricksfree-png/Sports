-- =============================================================================
-- stg_odds — Staging de odds de apostas com probabilidades implícitas
-- Camada: Staging | Materialização: view
-- Responsabilidade: cast explícito, cálculo de probabilidades implícitas (1/odd)
--   normalizadas para somar 1 (remove margem do bookmaker)
-- Grain: uma linha por fixture_id por bookmaker por mercado
-- =============================================================================
with source as (
    select
        odds_id,
        fixture_id,
        bookmaker_id,
        bookmaker_name,
        market_name,
        -- odds decimais brutas
        cast(odd_home as numeric(10, 4))     as odd_home,
        cast(odd_draw as numeric(10, 4))     as odd_draw,
        cast(odd_away as numeric(10, 4))     as odd_away,
        cast(loaded_at as timestamp)         as loaded_at
    from {{ source('football_raw', 'odds') }}
    where fixture_id is not null
      and odd_home  > 1
      and odd_draw  > 1
      and odd_away  > 1
),

with_raw_probs as (
    select
        *,
        -- Probabilidades brutas (incluem margem)
        round((1.0 / odd_home)::numeric, 6)  as raw_prob_home,
        round((1.0 / odd_draw)::numeric, 6)  as raw_prob_draw,
        round((1.0 / odd_away)::numeric, 6)  as raw_prob_away,
        -- Soma total (overround/margem do bookmaker)
        round(
            (1.0 / odd_home + 1.0 / odd_draw + 1.0 / odd_away)::numeric, 6
        )                                    as overround
    from source
),

with_implied_probs as (
    select
        *,
        -- Probabilidades implícitas normalizadas (sem margem)
        round((raw_prob_home / overround)::numeric, 6) as implied_prob_home,
        round((raw_prob_draw / overround)::numeric, 6) as implied_prob_draw,
        round((raw_prob_away / overround)::numeric, 6) as implied_prob_away
    from with_raw_probs
)

select
    odds_id,
    fixture_id,
    bookmaker_id,
    bookmaker_name,
    market_name,
    odd_home,
    odd_draw,
    odd_away,
    raw_prob_home,
    raw_prob_draw,
    raw_prob_away,
    overround,
    implied_prob_home,
    implied_prob_draw,
    implied_prob_away,
    loaded_at
from with_implied_probs
