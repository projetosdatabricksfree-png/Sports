"""
Prediction pipeline for upcoming fixtures.

Loads the champion model from the MLflow Model Registry, fetches fixtures
for the next DEFAULT_DAYS_AHEAD days and writes predictions to
marts.fct_predictions via an upsert (ON CONFLICT … DO UPDATE).

Usage
-----
    python predict.py [--days-ahead N] [--league LEAGUE_ID]
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent))

from config import (
    DATABASE_URL,
    DEFAULT_DAYS_AHEAD,
    FEATURE_COLUMNS,
    MLFLOW_TRACKING_URI,
    MODEL_REGISTRY_NAME,
)
from feature_engineering import FeatureEngineer
from hybrid_model import HybridModel
from poisson_model import PoissonModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger("predict")

# DDL to ensure the predictions table exists
_CREATE_TABLE_SQL = """
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS marts.fct_predictions (
    fixture_id              BIGINT      PRIMARY KEY,
    league_id               INT,
    match_date              DATE,
    home_team_id            INT,
    away_team_id            INT,
    poisson_home_prob       FLOAT,
    poisson_draw_prob       FLOAT,
    poisson_away_prob       FLOAT,
    gb_home_prob            FLOAT,
    gb_draw_prob            FLOAT,
    gb_away_prob            FLOAT,
    hybrid_home_prob        FLOAT,
    hybrid_draw_prob        FLOAT,
    hybrid_away_prob        FLOAT,
    predicted_result        VARCHAR(10),
    confidence_score        FLOAT,
    expected_home_goals     FLOAT,
    expected_away_goals     FLOAT,
    model_version           VARCHAR(50),
    predicted_at            TIMESTAMPTZ DEFAULT now()
);
"""

_UPSERT_SQL = text(
    """
    INSERT INTO marts.fct_predictions (
        fixture_id, league_id, match_date, home_team_id, away_team_id,
        poisson_home_prob, poisson_draw_prob, poisson_away_prob,
        gb_home_prob, gb_draw_prob, gb_away_prob,
        hybrid_home_prob, hybrid_draw_prob, hybrid_away_prob,
        predicted_result, confidence_score,
        expected_home_goals, expected_away_goals,
        model_version, predicted_at
    ) VALUES (
        :fixture_id, :league_id, :match_date, :home_team_id, :away_team_id,
        :poisson_home_prob, :poisson_draw_prob, :poisson_away_prob,
        :gb_home_prob, :gb_draw_prob, :gb_away_prob,
        :hybrid_home_prob, :hybrid_draw_prob, :hybrid_away_prob,
        :predicted_result, :confidence_score,
        :expected_home_goals, :expected_away_goals,
        :model_version, :predicted_at
    )
    ON CONFLICT (fixture_id) DO UPDATE SET
        league_id           = EXCLUDED.league_id,
        match_date          = EXCLUDED.match_date,
        home_team_id        = EXCLUDED.home_team_id,
        away_team_id        = EXCLUDED.away_team_id,
        poisson_home_prob   = EXCLUDED.poisson_home_prob,
        poisson_draw_prob   = EXCLUDED.poisson_draw_prob,
        poisson_away_prob   = EXCLUDED.poisson_away_prob,
        gb_home_prob        = EXCLUDED.gb_home_prob,
        gb_draw_prob        = EXCLUDED.gb_draw_prob,
        gb_away_prob        = EXCLUDED.gb_away_prob,
        hybrid_home_prob    = EXCLUDED.hybrid_home_prob,
        hybrid_draw_prob    = EXCLUDED.hybrid_draw_prob,
        hybrid_away_prob    = EXCLUDED.hybrid_away_prob,
        predicted_result    = EXCLUDED.predicted_result,
        confidence_score    = EXCLUDED.confidence_score,
        expected_home_goals = EXCLUDED.expected_home_goals,
        expected_away_goals = EXCLUDED.expected_away_goals,
        model_version       = EXCLUDED.model_version,
        predicted_at        = EXCLUDED.predicted_at
    """
)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate predictions for upcoming fixtures."
    )
    parser.add_argument(
        "--days-ahead",
        type=int,
        default=DEFAULT_DAYS_AHEAD,
        help=f"Prediction horizon in days (default: {DEFAULT_DAYS_AHEAD}).",
    )
    parser.add_argument(
        "--league",
        type=int,
        default=None,
        help="Restrict to a single league ID.",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Model loading
# ---------------------------------------------------------------------------

def _load_champion_model() -> tuple:
    """
    Load the latest champion model from the MLflow Model Registry.

    Returns
    -------
    (sklearn_pipeline, model_version_str)
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = mlflow.tracking.MlflowClient()

    versions = client.get_latest_versions(MODEL_REGISTRY_NAME, stages=["Production"])
    if not versions:
        # Fall back to any latest version
        versions = client.get_latest_versions(MODEL_REGISTRY_NAME)

    if not versions:
        raise RuntimeError(
            f"No registered model found for '{MODEL_REGISTRY_NAME}'. "
            "Run train.py first."
        )

    version = versions[0]
    model_uri = f"models:/{MODEL_REGISTRY_NAME}/{version.version}"
    logger.info("Loading model from registry: %s (version %s)", MODEL_REGISTRY_NAME, version.version)
    loaded = mlflow.sklearn.load_model(model_uri)
    return loaded, str(version.version)


def _load_poisson_model() -> Optional[PoissonModel]:
    """
    Try to load the last saved PoissonModel JSON from the artifacts folder.
    Returns None if not found.
    """
    artifact_path = Path(__file__).parent / "artifacts" / "poisson_model.json"
    if artifact_path.exists():
        logger.info("Loading PoissonModel from %s", artifact_path)
        return PoissonModel.load(artifact_path)
    logger.warning("PoissonModel artifact not found at %s. Poisson probs will be uniform.", artifact_path)
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: Optional[list] = None) -> None:
    args = parse_args(argv)

    # Load champion GB model from registry
    gb_sklearn, model_version = _load_champion_model()
    poisson_model = _load_poisson_model()

    # Wrap the sklearn model inside GradientBoostingModel interface
    from gradient_boosting_model import GradientBoostingModel
    gb_wrapper = GradientBoostingModel()
    gb_wrapper._model = gb_sklearn
    gb_wrapper._is_fitted = True

    # Build hybrid model (no re-calibration at inference time)
    hybrid = HybridModel(
        poisson_model=poisson_model,
        gb_model=gb_wrapper,
        poisson_weight=0.3,
        gb_weight=0.7,
    )

    # Load upcoming fixtures
    fe = FeatureEngineer()
    fixtures_df = fe.get_upcoming_fixtures(days_ahead=args.days_ahead)

    if args.league is not None:
        fixtures_df = fixtures_df[fixtures_df["league_id"] == args.league]

    if fixtures_df.empty:
        logger.info("No upcoming fixtures found for the next %d days.", args.days_ahead)
        return

    logger.info("Generating predictions for %d fixtures…", len(fixtures_df))

    now = datetime.now(tz=timezone.utc)
    records: list[dict] = []

    for _, row in fixtures_df.iterrows():
        fixture_data = row.to_dict()
        try:
            pred = hybrid.predict(fixture_data)
        except Exception as exc:
            logger.warning(
                "Prediction failed for fixture %s: %s",
                fixture_data.get("fixture_id"),
                exc,
            )
            continue

        records.append(
            {
                "fixture_id": int(fixture_data["fixture_id"]),
                "league_id": fixture_data.get("league_id"),
                "match_date": fixture_data.get("match_date"),
                "home_team_id": int(fixture_data["home_team_id"]),
                "away_team_id": int(fixture_data["away_team_id"]),
                "poisson_home_prob": pred["poisson_home_prob"],
                "poisson_draw_prob": pred["poisson_draw_prob"],
                "poisson_away_prob": pred["poisson_away_prob"],
                "gb_home_prob": pred["gb_home_prob"],
                "gb_draw_prob": pred["gb_draw_prob"],
                "gb_away_prob": pred["gb_away_prob"],
                "hybrid_home_prob": pred["hybrid_home_prob"],
                "hybrid_draw_prob": pred["hybrid_draw_prob"],
                "hybrid_away_prob": pred["hybrid_away_prob"],
                "predicted_result": pred["predicted_result"],
                "confidence_score": pred["confidence_score"],
                "expected_home_goals": pred["expected_home_goals"],
                "expected_away_goals": pred["expected_away_goals"],
                "model_version": model_version,
                "predicted_at": now,
            }
        )

    if not records:
        logger.warning("No valid predictions generated.")
        return

    # Persist to DB
    engine = fe.engine
    with engine.begin() as conn:
        conn.execute(text(_CREATE_TABLE_SQL))
        for rec in records:
            conn.execute(_UPSERT_SQL, rec)

    logger.info(
        "Upserted %d prediction(s) into marts.fct_predictions.", len(records)
    )


if __name__ == "__main__":
    main()
