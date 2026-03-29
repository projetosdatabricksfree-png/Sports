-- =============================================================================
-- PostgreSQL — Schemas e tabelas para Football Prediction Platform
-- API-Football: https://dashboard.api-football.com/
-- =============================================================================

-- Banco de metadados do MLflow
CREATE DATABASE mlflow_metadata;
GRANT ALL PRIVILEGES ON DATABASE mlflow_metadata TO admin;

\c dataplatform;

-- =============================================================================
-- RAW — Tabelas de ingestão da API-Football
-- =============================================================================

-- Fixtures (partidas) de todas as ligas
CREATE TABLE IF NOT EXISTS raw.fixtures (
    fixture_id          INTEGER PRIMARY KEY,
    league_id           INTEGER NOT NULL,
    league_name         VARCHAR(100),
    season              INTEGER NOT NULL,
    round               VARCHAR(50),
    match_date          TIMESTAMP,
    status              VARCHAR(10),
    home_team_id        INTEGER,
    home_team_name      VARCHAR(100),
    away_team_id        INTEGER,
    away_team_name      VARCHAR(100),
    home_goals          INTEGER,
    away_goals          INTEGER,
    home_goals_ht       INTEGER,
    away_goals_ht       INTEGER,
    venue_name          VARCHAR(150),
    referee             VARCHAR(100),
    loaded_at           TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fixtures_league_season ON raw.fixtures(league_id, season);
CREATE INDEX IF NOT EXISTS idx_fixtures_match_date ON raw.fixtures(match_date);
CREATE INDEX IF NOT EXISTS idx_fixtures_teams ON raw.fixtures(home_team_id, away_team_id);

-- Classificações por liga e temporada
CREATE TABLE IF NOT EXISTS raw.standings (
    standing_id         VARCHAR(100) PRIMARY KEY,  -- league_id|season|team_id
    league_id           INTEGER NOT NULL,
    league_name         VARCHAR(100),
    season              INTEGER NOT NULL,
    team_id             INTEGER NOT NULL,
    team_name           VARCHAR(100),
    rank                INTEGER,
    points              INTEGER,
    played              INTEGER,
    won                 INTEGER,
    drawn               INTEGER,
    lost                INTEGER,
    goals_for           INTEGER,
    goals_against       INTEGER,
    goal_diff           INTEGER,
    form                VARCHAR(20),
    loaded_at           TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_standings_league_season ON raw.standings(league_id, season);

-- Times de todas as ligas
CREATE TABLE IF NOT EXISTS raw.teams_football (
    team_id             INTEGER PRIMARY KEY,
    team_name           VARCHAR(100) NOT NULL,
    team_code           VARCHAR(10),
    country             VARCHAR(100),
    founded             INTEGER,
    national            BOOLEAN DEFAULT FALSE,
    logo_url            VARCHAR(300),
    venue_name          VARCHAR(150),
    venue_capacity      INTEGER,
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- Odds por fixture
CREATE TABLE IF NOT EXISTS raw.odds (
    odd_id              VARCHAR(100) PRIMARY KEY,  -- fixture_id|bookmaker_id|market
    fixture_id          INTEGER NOT NULL,
    bookmaker_id        INTEGER,
    bookmaker_name      VARCHAR(100),
    market              VARCHAR(50),
    home_odd            DECIMAL(8,3),
    draw_odd            DECIMAL(8,3),
    away_odd            DECIMAL(8,3),
    loaded_at           TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_odds_fixture ON raw.odds(fixture_id);

-- =============================================================================
-- MARTS — Tabelas de predições (preenchidas pelo ML Python)
-- =============================================================================

CREATE TABLE IF NOT EXISTS marts.fct_predictions (
    fixture_id              INTEGER PRIMARY KEY,
    league_id               INTEGER,
    match_date              TIMESTAMP,
    home_team_id            INTEGER,
    home_team_name          VARCHAR(100),
    away_team_id            INTEGER,
    away_team_name          VARCHAR(100),
    -- Modelo Poisson
    poisson_home_prob       DECIMAL(6,4),
    poisson_draw_prob       DECIMAL(6,4),
    poisson_away_prob       DECIMAL(6,4),
    expected_home_goals     DECIMAL(5,2),
    expected_away_goals     DECIMAL(5,2),
    -- Modelo Gradient Boosting
    gb_home_prob            DECIMAL(6,4),
    gb_draw_prob            DECIMAL(6,4),
    gb_away_prob            DECIMAL(6,4),
    -- Modelo Híbrido (final)
    hybrid_home_prob        DECIMAL(6,4),
    hybrid_draw_prob        DECIMAL(6,4),
    hybrid_away_prob        DECIMAL(6,4),
    predicted_result        VARCHAR(10),   -- HOME_WIN, DRAW, AWAY_WIN
    confidence_score        DECIMAL(5,4),
    model_version           VARCHAR(50),
    predicted_at            TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_predictions_league ON marts.fct_predictions(league_id);
CREATE INDEX IF NOT EXISTS idx_predictions_date ON marts.fct_predictions(match_date);

-- Grants
GRANT ALL ON ALL TABLES IN SCHEMA raw TO admin;
GRANT ALL ON ALL TABLES IN SCHEMA marts TO admin;
GRANT ALL ON SCHEMA raw TO admin;
