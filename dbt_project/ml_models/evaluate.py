"""
Model evaluation script — compares past predictions with actual results.

Reads:
  - marts.fct_predictions  (model output)
  - marts.fct_match_features (ground truth)

Produces:
  - Overall and per-league metrics (accuracy, log_loss, brier_score, ROI)
  - JSON report saved under dbt_project/ml_models/reports/

Usage
-----
    python evaluate.py [--league LEAGUE_ID] [--since YYYY-MM-DD] [--output DIR]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent))

from config import DATABASE_URL, INVERSE_TARGET_MAP, TARGET_MAP

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger("evaluate")

_DEFAULT_REPORTS_DIR = Path(__file__).parent / "reports"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate historical predictions against real results."
    )
    parser.add_argument("--league", type=int, default=None, help="Filter to a single league.")
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Only evaluate matches on or after this date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(_DEFAULT_REPORTS_DIR),
        help="Directory where the JSON report will be saved.",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_merged(
    engine,
    league_id: Optional[int],
    since: Optional[str],
) -> pd.DataFrame:
    """
    Join fct_predictions with fct_match_features on fixture_id and return
    rows where both prediction and actual result exist.
    """
    conditions = ["f.match_result IS NOT NULL", "p.fixture_id IS NOT NULL"]
    params: dict = {}

    if league_id is not None:
        conditions.append("p.league_id = :league_id")
        params["league_id"] = league_id

    if since is not None:
        conditions.append("f.match_date >= :since")
        params["since"] = since

    where_clause = " AND ".join(conditions)
    query = text(
        f"""
        SELECT
            p.fixture_id,
            p.league_id,
            f.match_date,
            p.predicted_result,
            f.match_result          AS actual_result,
            p.hybrid_home_prob,
            p.hybrid_draw_prob,
            p.hybrid_away_prob,
            p.confidence_score,
            p.model_version
        FROM   marts.fct_predictions   p
        JOIN   marts.fct_match_features f USING (fixture_id)
        WHERE  {where_clause}
        ORDER  BY f.match_date ASC
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)
    return df


# ---------------------------------------------------------------------------
# Metric computation
# ---------------------------------------------------------------------------

def _compute_metrics(df: pd.DataFrame) -> dict:
    """
    Compute accuracy, log_loss, brier_score and flat-bet ROI for *df*.

    Flat-bet ROI: We simulate a flat €1 bet on every predicted_result.
    Win = +2 (1 return + 1 profit at evens); Loss = -1.
    Real bookmaker odds would improve this simulation.
    """
    if df.empty:
        return {}

    y_true = df["actual_result"].map(TARGET_MAP).astype(int)
    y_pred_label = df["predicted_result"].map(TARGET_MAP).astype(int)

    proba = df[["hybrid_home_prob", "hybrid_draw_prob", "hybrid_away_prob"]].values.clip(
        1e-7, 1 - 1e-7
    )
    # Re-normalise
    proba = proba / proba.sum(axis=1, keepdims=True)

    acc = accuracy_score(y_true, y_pred_label)
    ll = log_loss(y_true, proba)
    brier = float(
        np.mean(
            [
                brier_score_loss((y_true == c).astype(int), proba[:, c])
                for c in range(3)
            ]
        )
    )

    correct_mask = y_true.values == y_pred_label.values
    n = len(df)
    n_correct = correct_mask.sum()
    n_wrong = n - n_correct
    roi = float((n_correct * 1.0 - n_wrong * 1.0) / n)  # profit per unit staked

    return {
        "n_matches": int(n),
        "accuracy": round(float(acc), 4),
        "log_loss": round(float(ll), 4),
        "brier_score": round(brier, 4),
        "flat_bet_roi": round(roi, 4),
        "n_correct": int(n_correct),
        "n_wrong": int(n_wrong),
    }


def _compute_per_league(df: pd.DataFrame) -> dict:
    """Return metrics broken down by league_id."""
    per_league: dict = {}
    for league_id, group in df.groupby("league_id"):
        per_league[int(league_id)] = _compute_metrics(group)
    return per_league


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: Optional[list] = None) -> None:
    args = parse_args(argv)
    engine = create_engine(DATABASE_URL)

    logger.info(
        "Loading predictions (league=%s, since=%s)…",
        args.league or "ALL",
        args.since or "ALL",
    )
    df = _load_merged(engine, args.league, args.since)

    if df.empty:
        logger.warning("No matched predictions found. Nothing to evaluate.")
        return

    logger.info("Evaluating %d prediction(s)…", len(df))

    overall = _compute_metrics(df)
    per_league = _compute_per_league(df)

    # Model versions present
    model_versions = df["model_version"].dropna().unique().tolist()

    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "filters": {
            "league": args.league,
            "since": args.since,
        },
        "model_versions": model_versions,
        "overall": overall,
        "per_league": per_league,
    }

    # Save report
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    report_path = output_dir / f"evaluation_{timestamp}.json"
    report_path.write_text(json.dumps(report, indent=2))

    logger.info("Report saved to %s", report_path)
    logger.info("Overall metrics: %s", overall)

    # Pretty-print summary
    print("\n=== Evaluation Summary ===")
    for k, v in overall.items():
        print(f"  {k:<20} {v}")

    if per_league:
        print("\n=== Per-League Metrics ===")
        for lid, m in per_league.items():
            print(f"\n  League {lid}:")
            for k, v in m.items():
                print(f"    {k:<20} {v}")


if __name__ == "__main__":
    main()
