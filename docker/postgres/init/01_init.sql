-- =============================================================================
-- PostgreSQL — Inicialização dos bancos do data platform
-- =============================================================================

-- Banco para metadados do Airflow
CREATE DATABASE airflow_metadata;
GRANT ALL PRIVILEGES ON DATABASE airflow_metadata TO admin;

-- Banco para metadados do Superset
CREATE DATABASE superset_metadata;
GRANT ALL PRIVILEGES ON DATABASE superset_metadata TO admin;

-- Schemas do dbt (staging, intermediate, marts, snapshots)
\c dataplatform;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS snapshots;

GRANT ALL ON SCHEMA raw, staging, intermediate, marts, snapshots TO admin;

-- Tabela raw de exemplo para o caso de uso Sports Analytics
CREATE TABLE IF NOT EXISTS raw.matches (
    match_id        VARCHAR(36) PRIMARY KEY,
    match_date      DATE NOT NULL,
    home_team_id    VARCHAR(36),
    away_team_id    VARCHAR(36),
    home_goals      INTEGER,
    away_goals      INTEGER,
    competition_id  VARCHAR(36),
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.teams (
    team_id         VARCHAR(36) PRIMARY KEY,
    team_name       VARCHAR(100) NOT NULL,
    country         VARCHAR(50),
    founded_year    INTEGER,
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.team_roster (
    player_id       VARCHAR(36) PRIMARY KEY,
    team_id         VARCHAR(36),
    player_name     VARCHAR(100),
    position        VARCHAR(30),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Dados de seed para testes
INSERT INTO raw.teams VALUES
    ('t001', 'Flamengo',     'Brazil', 1895, NOW()),
    ('t002', 'Corinthians',  'Brazil', 1910, NOW()),
    ('t003', 'Palmeiras',    'Brazil', 1914, NOW()),
    ('t004', 'São Paulo FC', 'Brazil', 1930, NOW())
ON CONFLICT (team_id) DO NOTHING;

INSERT INTO raw.matches VALUES
    ('m001', '2024-01-15', 't001', 't002', 2, 1, 'c001', NOW()),
    ('m002', '2024-01-22', 't003', 't004', 0, 0, 'c001', NOW()),
    ('m003', '2024-02-01', 't002', 't003', 1, 3, 'c001', NOW()),
    ('m004', '2024-02-08', 't004', 't001', 1, 2, 'c001', NOW()),
    ('m005', '2024-02-15', 't001', 't003', 1, 1, 'c001', NOW())
ON CONFLICT (match_id) DO NOTHING;
