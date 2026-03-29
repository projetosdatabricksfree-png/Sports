"""
ingestion/ingest_fixtures.py
Fetches fixtures from API-Football and upserts them into raw.fixtures.

Table schema (created automatically if absent)
-----------------------------------------------
fixture_id       BIGINT  PRIMARY KEY
league_id        INT
league_name      TEXT
season           INT
round            TEXT
match_date       TIMESTAMPTZ
status           TEXT
home_team_id     INT
home_team_name   TEXT
away_team_id     INT
away_team_name   TEXT
home_goals       INT
away_goals       INT
home_goals_ht    INT
away_goals_ht    INT
venue_name       TEXT
referee          TEXT
loaded_at        TIMESTAMPTZ
"""

import logging
import sys
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

from ingestion.api_client import FootballAPIClient
from ingestion.config import DATABASE_URL, LEAGUES, SEASON

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
DDL_CREATE_TABLE = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.fixtures (
    fixture_id      BIGINT PRIMARY KEY,
    league_id       INT,
    league_name     TEXT,
    season          INT,
    round           TEXT,
    match_date      TIMESTAMPTZ,
    status          TEXT,
    home_team_id    INT,
    home_team_name  TEXT,
    away_team_id    INT,
    away_team_name  TEXT,
    home_goals      INT,
    away_goals      INT,
    home_goals_ht   INT,
    away_goals_ht   INT,
    venue_name      TEXT,
    referee         TEXT,
    loaded_at       TIMESTAMPTZ
);
"""

UPSERT_SQL = """
INSERT INTO raw.fixtures (
    fixture_id, league_id, league_name, season, round, match_date, status,
    home_team_id, home_team_name, away_team_id, away_team_name,
    home_goals, away_goals, home_goals_ht, away_goals_ht,
    venue_name, referee, loaded_at
)
VALUES %s
ON CONFLICT (fixture_id) DO UPDATE SET
    league_id      = EXCLUDED.league_id,
    league_name    = EXCLUDED.league_name,
    season         = EXCLUDED.season,
    round          = EXCLUDED.round,
    match_date     = EXCLUDED.match_date,
    status         = EXCLUDED.status,
    home_team_id   = EXCLUDED.home_team_id,
    home_team_name = EXCLUDED.home_team_name,
    away_team_id   = EXCLUDED.away_team_id,
    away_team_name = EXCLUDED.away_team_name,
    home_goals     = EXCLUDED.home_goals,
    away_goals     = EXCLUDED.away_goals,
    home_goals_ht  = EXCLUDED.home_goals_ht,
    away_goals_ht  = EXCLUDED.away_goals_ht,
    venue_name     = EXCLUDED.venue_name,
    referee        = EXCLUDED.referee,
    loaded_at      = EXCLUDED.loaded_at;
"""


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _parse_fixtures(
    raw_fixtures: list[dict],
    league_id: int,
    league_name: str,
    season: int,
) -> list[tuple]:
    """Convert API response items into row tuples for psycopg2 execute_values."""
    rows = []
    loaded_at = datetime.now(tz=timezone.utc)

    for item in raw_fixtures:
        fix      = item.get("fixture", {})
        teams    = item.get("teams",   {})
        goals    = item.get("goals",   {})
        score    = item.get("score",   {})
        venue    = fix.get("venue",    {})
        halftime = score.get("halftime", {})

        rows.append((
            fix.get("id"),
            league_id,
            league_name,
            season,
            item.get("league", {}).get("round"),
            fix.get("date"),                              # ISO string → psycopg2 converts
            fix.get("status", {}).get("long"),
            teams.get("home", {}).get("id"),
            teams.get("home", {}).get("name"),
            teams.get("away", {}).get("id"),
            teams.get("away", {}).get("name"),
            goals.get("home"),
            goals.get("away"),
            halftime.get("home"),
            halftime.get("away"),
            venue.get("name"),
            fix.get("referee"),
            loaded_at,
        ))

    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def ingest_fixtures(
    leagues: dict[str, int] | None = None,
    season: int = SEASON,
) -> None:
    """
    Ingest fixtures for every league in *leagues* dict (name -> id).
    Defaults to all leagues defined in config.LEAGUES.
    """
    if leagues is None:
        leagues = LEAGUES

    client = FootballAPIClient()

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            cur.execute(DDL_CREATE_TABLE)
        conn.commit()
        logger.info("Schema/table ensured: raw.fixtures")

        for league_name, league_id in leagues.items():
            logger.info(
                "Fetching fixtures — league=%s (id=%d) season=%d",
                league_name, league_id, season,
            )
            try:
                raw = client.get_fixtures(league_id=league_id, season=season)
            except Exception as exc:
                logger.error(
                    "Failed to fetch fixtures for %s: %s", league_name, exc
                )
                continue

            if not raw:
                logger.warning(
                    "No fixtures returned for %s (id=%d) season=%d",
                    league_name, league_id, season,
                )
                continue

            rows = _parse_fixtures(raw, league_id, league_name, season)

            try:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(cur, UPSERT_SQL, rows)
                conn.commit()
                logger.info(
                    "Upserted %d fixtures for %s", len(rows), league_name
                )
            except Exception as exc:
                conn.rollback()
                logger.error(
                    "DB error upserting fixtures for %s: %s", league_name, exc
                )

    finally:
        conn.close()
        logger.info("Fixtures ingestion complete.")


if __name__ == "__main__":
    ingest_fixtures()
