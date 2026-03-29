"""
ingestion/run_all.py
Orchestrator — runs the four ingestion scripts in sequence.

Usage
-----
# All leagues
python -m ingestion.run_all

# Single league (name must match a key in config.LEAGUES)
python -m ingestion.run_all --league "Brasileirao Serie A"

# Multiple leagues
python -m ingestion.run_all --league "Premier League" --league "La Liga"

# Skip a step
python -m ingestion.run_all --skip odds
"""

import argparse
import logging
import sys
import time
from datetime import timedelta

from ingestion.config import LEAGUES, SEASON
from ingestion.ingest_fixtures  import ingest_fixtures
from ingestion.ingest_standings import ingest_standings
from ingestion.ingest_teams     import ingest_teams
from ingestion.ingest_odds      import ingest_odds

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("run_all")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _hms(seconds: float) -> str:
    """Format elapsed seconds as H:MM:SS."""
    return str(timedelta(seconds=round(seconds)))


def _run_step(name: str, fn, *args, **kwargs) -> float:
    """
    Execute a single ingestion step, measure wall-clock time, and log.
    Returns elapsed seconds.
    """
    logger.info("=" * 60)
    logger.info("STARTING step: %s", name)
    t0 = time.monotonic()
    try:
        fn(*args, **kwargs)
        elapsed = time.monotonic() - t0
        logger.info("FINISHED step: %s  (elapsed %s)", name, _hms(elapsed))
    except Exception as exc:
        elapsed = time.monotonic() - t0
        logger.error(
            "FAILED step: %s after %s — %s", name, _hms(elapsed), exc,
            exc_info=True,
        )
    return elapsed


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run all API-Football ingestion steps.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Available leagues:\n  " + "\n  ".join(LEAGUES.keys()),
    )
    parser.add_argument(
        "--league",
        dest="leagues",
        action="append",
        metavar="LEAGUE_NAME",
        default=None,
        help=(
            "Filter to one or more leagues by name (repeatable). "
            "Must match a key in config.LEAGUES exactly."
        ),
    )
    parser.add_argument(
        "--season",
        type=int,
        default=SEASON,
        help=f"Season year (default: {SEASON})",
    )
    parser.add_argument(
        "--skip",
        dest="skips",
        action="append",
        metavar="STEP",
        default=[],
        choices=["fixtures", "standings", "teams", "odds"],
        help="Skip one or more steps (repeatable). Choices: fixtures standings teams odds",
    )
    parser.add_argument(
        "--days-ahead",
        type=int,
        default=7,
        help="How many days ahead to look for odds (default: 7)",
    )
    return parser.parse_args()


def _resolve_leagues(requested: list[str] | None) -> dict[str, int]:
    """Return the subset of LEAGUES matching *requested*, or all leagues."""
    if not requested:
        return LEAGUES

    resolved: dict[str, int] = {}
    for name in requested:
        if name in LEAGUES:
            resolved[name] = LEAGUES[name]
        else:
            logger.warning(
                "League '%s' not found in config.LEAGUES — skipping.", name
            )

    if not resolved:
        logger.error(
            "None of the requested leagues matched config.LEAGUES. "
            "Available: %s",
            ", ".join(LEAGUES.keys()),
        )
        sys.exit(1)

    return resolved


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args    = _parse_args()
    leagues = _resolve_leagues(args.leagues)
    season  = args.season
    skips   = set(args.skips)

    logger.info("run_all started")
    logger.info(
        "Leagues: %s | Season: %d | Skip: %s",
        list(leagues.keys()),
        season,
        skips or "none",
    )

    wall_start = time.monotonic()
    timings: dict[str, float] = {}

    # ---- Step 1: Fixtures -----------------------------------------------
    if "fixtures" not in skips:
        timings["fixtures"] = _run_step(
            "ingest_fixtures",
            ingest_fixtures,
            leagues=leagues,
            season=season,
        )
    else:
        logger.info("Skipping step: fixtures")

    # ---- Step 2: Standings ----------------------------------------------
    if "standings" not in skips:
        timings["standings"] = _run_step(
            "ingest_standings",
            ingest_standings,
            leagues=leagues,
            season=season,
        )
    else:
        logger.info("Skipping step: standings")

    # ---- Step 3: Teams --------------------------------------------------
    if "teams" not in skips:
        timings["teams"] = _run_step(
            "ingest_teams",
            ingest_teams,
            leagues=leagues,
            season=season,
        )
    else:
        logger.info("Skipping step: teams")

    # ---- Step 4: Odds ---------------------------------------------------
    if "odds" not in skips:
        timings["odds"] = _run_step(
            "ingest_odds",
            ingest_odds,
            days_ahead=args.days_ahead,
        )
    else:
        logger.info("Skipping step: odds")

    # ---- Summary --------------------------------------------------------
    total = time.monotonic() - wall_start
    logger.info("=" * 60)
    logger.info("run_all COMPLETE — total wall time: %s", _hms(total))
    logger.info("Per-step timings:")
    for step, elapsed in timings.items():
        logger.info("  %-15s %s", step, _hms(elapsed))
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
