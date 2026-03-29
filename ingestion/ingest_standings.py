"""
ingestion/ingest_standings.py
Fetches league standings from API-Football and upserts them into raw.standings.

Table schema (created automatically if absent)
-----------------------------------------------
standing_id      BIGINT  PRIMARY KEY  (generated as league_id * 1_000_000 + season * 10_000 + rank)
league_id        INT
league_name      TEXT
season           INT
team_id          INT
team_name        TEXT
rank             INT
points           INT
played           INT
won              INT
drawn            INT
lost             INT
goals_for        INT
goals_against    INT
goal_diff        INT
form             TEXT
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

CREATE TABLE IF NOT EXISTS raw.standings (
    standing_id     BIGINT PRIMARY KEY,
    league_id       INT,
    league_name     TEXT,
    season          INT,
    team_id         INT,
    team_name       TEXT,
    rank            INT,
    points          INT,
    played          INT,
    won             INT,
    drawn           INT,
    lost            INT,
    goals_for       INT,
    goals_against   INT,
    goal_diff       INT,
    form            TEXT,
    loaded_at       TIMESTAMPTZ
);
"""

UPSERT_SQL = """
INSERT INTO raw.standings (
    standing_id, league_id, league_name, season,
    team_id, team_name, rank, points,
    played, won, drawn, lost,
    goals_for, goals_against, goal_diff, form, loaded_at
)
VALUES %s
ON CONFLICT (standing_id) DO UPDATE SET
    league_id     = EXCLUDED.league_id,
    league_name   = EXCLUDED.league_name,
    season        = EXCLUDED.season,
    team_id       = EXCLUDED.team_id,
    team_name     = EXCLUDED.team_name,
    rank          = EXCLUDED.rank,
    points        = EXCLUDED.points,
    played        = EXCLUDED.played,
    won           = EXCLUDED.won,
    drawn         = EXCLUDED.drawn,
    lost          = EXCLUDED.lost,
    goals_for     = EXCLUDED.goals_for,
    goals_against = EXCLUDED.goals_against,
    goal_diff     = EXCLUDED.goal_diff,
    form          = EXCLUDED.form,
    loaded_at     = EXCLUDED.loaded_at;
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_standing_id(league_id: int, season: int, rank: int) -> int:
    """
    Deterministic surrogate key:
        league_id * 10_000_000 + season * 1_000 + rank

    Supports up to 999 teams per league (rank 1–999) and seasons up to 9999.
    """
    return league_id * 10_000_000 + season * 1_000 + rank


def _parse_standings(
    response: list[dict],
    league_id: int,
    league_name: str,
    season: int,
) -> list[tuple]:
    rows = []
    loaded_at = datetime.now(tz=timezone.utc)

    # Response structure: list of league objects, each containing standings arrays
    for league_block in response:
        league_data = league_block.get("league", {})
        standings_groups = league_data.get("standings", [])

        for group in standings_groups:      # group = one list (regular, relegation…)
            for entry in group:
                team     = entry.get("team",       {})
                all_s    = entry.get("all",         {})
                goals    = all_s.get("goals",       {})
                rank     = entry.get("rank", 0)
                team_id  = team.get("id")

                standing_id = _make_standing_id(league_id, season, rank)

                rows.append((
                    standing_id,
                    league_id,
                    league_name,
                    season,
                    team_id,
                    team.get("name"),
                    rank,
                    entry.get("points"),
                    all_s.get("played"),
                    all_s.get("win"),
                    all_s.get("draw"),
                    all_s.get("lose"),
                    goals.get("for"),
                    goals.get("against"),
                    entry.get("goalsDiff"),
                    entry.get("form"),
                    loaded_at,
                ))

    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def ingest_standings(
    leagues: dict[str, int] | None = None,
    season: int = SEASON,
) -> None:
    if leagues is None:
        leagues = LEAGUES

    client = FootballAPIClient()
    conn   = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            cur.execute(DDL_CREATE_TABLE)
        conn.commit()
        logger.info("Schema/table ensured: raw.standings")

        for league_name, league_id in leagues.items():
            logger.info(
                "Fetching standings — league=%s (id=%d) season=%d",
                league_name, league_id, season,
            )
            try:
                response = client.get_standings(league_id=league_id, season=season)
            except Exception as exc:
                logger.error(
                    "Failed to fetch standings for %s: %s", league_name, exc
                )
                continue

            if not response:
                logger.warning(
                    "No standings returned for %s (id=%d) season=%d",
                    league_name, league_id, season,
                )
                continue

            rows = _parse_standings(response, league_id, league_name, season)

            if not rows:
                logger.warning("Parsed 0 rows from standings for %s", league_name)
                continue

            try:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(cur, UPSERT_SQL, rows)
                conn.commit()
                logger.info(
                    "Upserted %d standing rows for %s", len(rows), league_name
                )
            except Exception as exc:
                conn.rollback()
                logger.error(
                    "DB error upserting standings for %s: %s", league_name, exc
                )

    finally:
        conn.close()
        logger.info("Standings ingestion complete.")


if __name__ == "__main__":
    ingest_standings()
