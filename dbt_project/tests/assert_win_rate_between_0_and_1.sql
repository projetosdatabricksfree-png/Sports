-- =============================================================================
-- Teste singular: win_rate deve estar entre 0 e 1
-- =============================================================================
select team_id, win_rate
from {{ ref('fct_team_season_stats') }}
where win_rate < 0
   or win_rate > 1
