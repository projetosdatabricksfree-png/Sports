"""
ingestion/ingest_odds.py
Fetches Match-Winner odds (bet365) for upcoming fixtures and upserts
them into raw.odds.

Strategy
--------
1. Query raw.fixtures for matches scheduled within the next 7 days.
2. For each fixture_id, call GET /odds?fixture=<id>&bookmaker=1&bet=1.
3. Extract Home / Draw / Away odds for the "Match Winner" market.
4. Upsert into raw.odds.

Table schema (created automatically if absent)
-----------------------------------------------
odd_id          BIGINT  PRIMARY KEY  (= fixture_id * 100 + bookmaker_id)
fixture_id      BIGINT
bookmaker_id    INT
bookmaker_name  TEXT
market          TEXT
home_odd        NUMERIC(8,3)
draw_odd        NUMERIC(8,3)
away_odd        NUMERIC(8,3)
loaded_at       TIMESTAMPTZ
"""

import logging
import sys
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras

from ingestion.api_client import FootballAPIClient
from ingestion.config import DATABASE_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Bet365 bookmaker ID in API-Football
BET365_ID   = 1
# Bet ID 1 == "Match Winner" in API-Football
MATCH_WINNER_BET_ID = 1

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
DDL_CREATE_TABLE = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.odds (
    odd_id          BIGINT PRIMARY KEY,
    fixture_id      BIGINT,
    bookmaker_id    INT,
    bookmaker_name  TEXT,
    market          TEXT,
    home_odd        NUMERIC(8, 3),
    draw_odd        NUMERIC(8, 3),
    away_odd        NUMERIC(8, 3),
    loaded_at       TIMESTAMPTZ
);
"""

UPSERT_SQL = """
INSERT INTO raw.odds (
    odd_id, fixture_id, bookmaker_id, bookmaker_name,
    market, home_odd, draw_odd, away_odd, loaded_at
)
VALUES %s
ON CONFLICT (odd_id) DO UPDATE SET
    bookmaker_name = EXCLUDED.bookmaker_name,
    market         = EXCLUDED.market,
    home_odd       = EXCLUDED.home_odd,
    draw_odd       = EXCLUDED.draw_odd,
    away_odd       = EXCLUDED.away_odd,
    loaded_at      = EXCLUDED.loaded_at;
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_odd_id(fixture_id: int, bookmaker_id: int) -> int:
    """Deterministic surrogate: fixture_id * 1000 + bookmaker_id."""
    return fixture_id * 1_000 + bookmaker_id


def _fetch_upcoming_fixture_ids(conn: psycopg2.extensions.connection, days: int = 7) -> list[int]:
    """
    Read fixture IDs from raw.fixtures where the match is scheduled
    within the next *days* days and the match has not yet kicked off
    (status NOT IN ('FT','AET','PEN','CANC','AWD','WO')).
    """
    now     = datetime.now(tz=timezone.utc)
    cutoff  = now + timedelta(days=days)
    terminal_statuses = ("FT", "AET", "PEN", "CANC", "AWD", "WO", "ABD", "PST")

    query = """
        SELECT fixture_id
        FROM   raw.fixtures
        WHERE  match_date >= %(now)s
          AND  match_date <= %(cutoff)s
          AND  status NOT IN %(terminal)s
        ORDER  BY match_date
    """
    with conn.cursor() as cur:
        cur.execute(query, {"now": now, "cutoff": cutoff, "terminal": terminal_statuses})
        rows = cur.fetchall()

    fixture_ids = [r[0] for r in rows]
    logger.info("Found %d upcoming fixtures in the next %d days.", len(fixture_ids), days)
    return fixture_ids


def _parse_odds(
    response: list[dict],
    fixture_id: int,
) -> list[tuple] | None:
    """
    Extract the Match Winner market from the API response for a single fixture.
    Returns a list of row-tuples (usually one row per bookmaker) or None if
    the expected data is not present.
    """
    if not response:
        return None

    loaded_at = datetime.now(tz=timezone.utc)
    rows = []

    for item in response:
        bookmakers = item.get("bookmakers", [])
        for bm in bookmakers:
            bm_id   = bm.get("id")
            bm_name = bm.get("name", "")
            bets    = bm.get("bets", [])

            for bet in bets:
                if bet.get("name", "").lower() != "match winner":
                    continue

                values = {v["value"]: v["odd"] for v in bet.get("values", [])}
                home_odd = values.get("Home")
                draw_odd = values.get("Draw")
                away_odd = values.get("Away")

                if home_odd is None and draw_odd is None and away_odd is None:
                    continue

                try:
                    rows.append((
                        _make_odd_id(fixture_id, bm_id),
                        fixture_id,
                        bm_id,
                        bm_name,
                        "Match Winner",
                        float(home_odd) if home_odd is not None else None,
                        float(draw_odd) if draw_odd is not None else None,
                        float(away_odd) if away_odd is not None else None,
                        loaded_at,
                    ))
                except (ValueError, TypeError) as exc:
                    logger.warning(
                        "Could not parse odds for fixture %d bm %d: %s",
                        fixture_id, bm_id, exc,
                    )

    return rows if rows else None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def ingest_odds(days_ahead: int = 7) -> None:
    """
    Fetch and store Match Winner odds for fixtures in the next *days_ahead* days.
    """
    client = FootballAPIClient()
    conn   = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            cur.execute(DDL_CREATE_TABLE)
        conn.commit()
        logger.info("Schema/table ensured: raw.odds")

        # Step 1 — find which fixtures are upcoming
        try:
            fixture_ids = _fetch_upcoming_fixture_ids(conn, days=days_ahead)
        except psycopg2.errors.UndefinedTable:
            conn.rollback()
            logger.error(
                "raw.fixtures does not exist yet. Run ingest_fixtures first."
            )
            return

        if not fixture_ids:
            logger.info("No upcoming fixtures found — nothing to fetch.")
            return

        # Step 2 — fetch odds per fixture
        total_upserted = 0

        for fixture_id in fixture_ids:
            logger.debug("Fetching odds for fixture_id=%d", fixture_id)
            try:
                response = client.get_odds(
                    fixture_id=fixture_id,
                    bookmaker_id=BET365_ID,
                    bet_id=MATCH_WINNER_BET_ID,
                )
            except Exception as exc:
                logger.error(
                    "Failed to fetch odds for fixture %d: %s", fixture_id, exc
                )
                continue

            rows = _parse_odds(response, fixture_id)
            if not rows:
                logger.debug("No Match Winner odds found for fixture %d", fixture_id)
                continue

            try:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(cur, UPSERT_SQL, rows)
                conn.commit()
                total_upserted += len(rows)
            except Exception as exc:
                conn.rollback()
                logger.error(
                    "DB error upserting odds for fixture %d: %s", fixture_id, exc
                )

        logger.info("Odds ingestion complete — %d rows upserted.", total_upserted)

    finally:
        conn.close()


if __name__ == "__main__":
    ingest_odds()
