"""
ingestion/ingest_teams.py
Fetches team information from API-Football and upserts into raw.teams_football.

Team records are league-agnostic (same team may appear in multiple leagues).
We upsert by team_id so the most recently loaded version wins.

Table schema (created automatically if absent)
-----------------------------------------------
team_id          INT  PRIMARY KEY
team_name        TEXT
team_code        TEXT
country          TEXT
founded          INT
national         BOOLEAN
logo_url         TEXT
venue_name       TEXT
venue_capacity   INT
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

CREATE TABLE IF NOT EXISTS raw.teams_football (
    team_id         INT PRIMARY KEY,
    team_name       TEXT,
    team_code       TEXT,
    country         TEXT,
    founded         INT,
    national        BOOLEAN,
    logo_url        TEXT,
    venue_name      TEXT,
    venue_capacity  INT,
    loaded_at       TIMESTAMPTZ
);
"""

UPSERT_SQL = """
INSERT INTO raw.teams_football (
    team_id, team_name, team_code, country, founded,
    national, logo_url, venue_name, venue_capacity, loaded_at
)
VALUES %s
ON CONFLICT (team_id) DO UPDATE SET
    team_name      = EXCLUDED.team_name,
    team_code      = EXCLUDED.team_code,
    country        = EXCLUDED.country,
    founded        = EXCLUDED.founded,
    national       = EXCLUDED.national,
    logo_url       = EXCLUDED.logo_url,
    venue_name     = EXCLUDED.venue_name,
    venue_capacity = EXCLUDED.venue_capacity,
    loaded_at      = EXCLUDED.loaded_at;
"""


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _parse_teams(raw_teams: list[dict]) -> dict[int, tuple]:
    """
    Parse team + venue data from the /teams endpoint response.
    Returns a dict keyed by team_id to avoid duplicates within the same batch.
    """
    loaded_at = datetime.now(tz=timezone.utc)
    teams: dict[int, tuple] = {}

    for item in raw_teams:
        team  = item.get("team",  {})
        venue = item.get("venue", {})

        team_id = team.get("id")
        if team_id is None:
            continue

        teams[team_id] = (
            team_id,
            team.get("name"),
            team.get("code"),
            team.get("country"),
            team.get("founded"),
            team.get("national", False),
            team.get("logo"),
            venue.get("name"),
            venue.get("capacity"),
            loaded_at,
        )

    return teams


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def ingest_teams(
    leagues: dict[str, int] | None = None,
    season: int = SEASON,
) -> None:
    """
    Ingest team records for every league.
    Also attempts to fetch per-team statistics (logged as DEBUG; not stored
    in raw.teams_football to keep the schema lean — extend as needed).
    """
    if leagues is None:
        leagues = LEAGUES

    client = FootballAPIClient()
    conn   = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            cur.execute(DDL_CREATE_TABLE)
        conn.commit()
        logger.info("Schema/table ensured: raw.teams_football")

        # Accumulate unique teams across all leagues before writing
        all_teams: dict[int, tuple] = {}

        for league_name, league_id in leagues.items():
            logger.info(
                "Fetching teams — league=%s (id=%d) season=%d",
                league_name, league_id, season,
            )
            try:
                raw = client.get_teams(league_id=league_id, season=season)
            except Exception as exc:
                logger.error(
                    "Failed to fetch teams for %s: %s", league_name, exc
                )
                continue

            if not raw:
                logger.warning(
                    "No teams returned for %s (id=%d) season=%d",
                    league_name, league_id, season,
                )
                continue

            parsed = _parse_teams(raw)
            new_teams = {k: v for k, v in parsed.items() if k not in all_teams}
            all_teams.update(new_teams)

            logger.info(
                "Parsed %d teams from %s (%d new unique)",
                len(parsed), league_name, len(new_teams),
            )

        if not all_teams:
            logger.warning("No team data collected — nothing to write.")
            return

        rows = list(all_teams.values())
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, UPSERT_SQL, rows)
            conn.commit()
            logger.info("Upserted %d unique teams into raw.teams_football", len(rows))
        except Exception as exc:
            conn.rollback()
            logger.error("DB error upserting teams: %s", exc)

    finally:
        conn.close()
        logger.info("Teams ingestion complete.")


if __name__ == "__main__":
    ingest_teams()
