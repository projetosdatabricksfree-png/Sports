-- =============================================================================
-- Teste singular: gols nunca podem ser negativos
-- Falha se retornar qualquer linha
-- =============================================================================
select match_id, home_goals, away_goals
from {{ ref('fct_match_predictions') }}
where home_goals < 0
   or away_goals < 0
