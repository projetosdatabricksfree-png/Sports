"""
ingestion/config.py
Configuration for the API-Football ingestion layer.
"""

import os

# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
API_KEY: str = os.environ.get("API_FOOTBALL_KEY", "9f1290452cb4f3790301e23b6786f897")
BASE_URL: str = "https://v3.football.api-sports.io"

HEADERS: dict = {
    "x-apisports-key": API_KEY,
    "Accept": "application/json",
}

# ---------------------------------------------------------------------------
# Leagues
# ---------------------------------------------------------------------------
LEAGUES: dict[str, int] = {
    # National — Serie A / top flights
    "Brasileirao Serie A":       71,
    "Premier League":            39,
    "La Liga":                  140,
    "Serie A Italy":            135,
    "Bundesliga":                78,
    "Ligue 1":                   61,
    "Eredivisie":                88,
    "Primeira Liga":             94,
    "Liga Profesional Argentina": 128,
    "Liga MX":                  262,
    "MLS":                      253,
    # International — club competitions
    "UEFA Champions League":      2,
    "UEFA Europa League":          3,
    "Copa Libertadores":          13,
    "Copa Sulamericana":          11,
    # International — national team competitions
    "FIFA World Cup":              1,
    "Copa America":                9,
    "UEFA Euro":                   4,
}

# ---------------------------------------------------------------------------
# Season
# ---------------------------------------------------------------------------
SEASON: int = 2025

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def _build_database_url() -> str:
    """
    Prefer an explicit POSTGRES_URL env var.
    Fall back to assembling the DSN from individual POSTGRES_* vars
    that match the Docker-Compose environment used in this project.
    """
    explicit = os.environ.get("POSTGRES_URL")
    if explicit:
        return explicit

    host     = os.environ.get("POSTGRES_HOST",     "localhost")
    port     = os.environ.get("POSTGRES_PORT",     "5432")
    user     = os.environ.get("POSTGRES_USER",     "airflow")
    password = os.environ.get("POSTGRES_PASSWORD", "airflow")
    db       = os.environ.get("POSTGRES_DB",       "airflow")

    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


DATABASE_URL: str = _build_database_url()

# ---------------------------------------------------------------------------
# Rate-limiting
# ---------------------------------------------------------------------------
# Free tier: 100 requests / minute.  Leave a safety margin.
REQUESTS_PER_MINUTE: int = 30
# Max retries on transient HTTP errors
MAX_RETRIES: int = 3
