"""
Training pipeline for the Football Prediction Platform.

Usage
-----
    python train.py [--league LEAGUE_ID] [--season SEASON] [--experiment-name NAME]

Examples
--------
    # Train on all leagues, current season
    python train.py

    # Train only on Premier League (id=39), season 2023
    python train.py --league 39 --season 2023

    # Use a custom MLflow experiment name
    python train.py --experiment-name my_experiment
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
from pathlib import Path
from typing import Optional

import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn
import mlflow.xgboost
import numpy as np
import pandas as pd
from sklearn.metrics import ConfusionMatrixDisplay, confusion_matrix
from sklearn.model_selection import train_test_split

# Ensure local modules resolve regardless of cwd
sys.path.insert(0, str(Path(__file__).parent))

from config import (
    BASELINE_ACCURACY,
    FEATURE_COLUMNS,
    INVERSE_TARGET_MAP,
    MLFLOW_EXPERIMENT_NAME,
    MLFLOW_TRACKING_URI,
    MODEL_REGISTRY_NAME,
)
from feature_engineering import FeatureEngineer
from gradient_boosting_model import GradientBoostingModel
from hybrid_model import HybridModel
from poisson_model import PoissonModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger("train")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train football prediction models and register the champion."
    )
    parser.add_argument(
        "--league",
        type=int,
        default=None,
        help="API-Football league ID to train on. Omit to use all leagues.",
    )
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="Season year (e.g. 2023). Omit to use all available seasons.",
    )
    parser.add_argument(
        "--experiment-name",
        default=MLFLOW_EXPERIMENT_NAME,
        help=f"MLflow experiment name (default: {MLFLOW_EXPERIMENT_NAME}).",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Artefact helpers
# ---------------------------------------------------------------------------

def _plot_confusion_matrix(
    y_true: pd.Series,
    y_pred: np.ndarray,
    run_id: str,
) -> None:
    """Build and log a confusion-matrix PNG to MLflow."""
    labels = [INVERSE_TARGET_MAP[i] for i in range(3)]
    cm = confusion_matrix(y_true, y_pred)
    fig, ax = plt.subplots(figsize=(6, 5))
    disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=labels)
    disp.plot(ax=ax, colorbar=False)
    ax.set_title("Confusion Matrix — Hybrid Model")
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120)
    buf.seek(0)

    tmp = Path(f"/tmp/confusion_matrix_{run_id}.png")
    tmp.write_bytes(buf.read())
    mlflow.log_artifact(str(tmp), artifact_path="plots")
    plt.close(fig)
    tmp.unlink(missing_ok=True)


def _plot_feature_importance(gb_model: GradientBoostingModel, run_id: str) -> None:
    """Plot XGBoost feature importances and log to MLflow."""
    importances = gb_model.model.feature_importances_
    indices = np.argsort(importances)[::-1]
    feats = [FEATURE_COLUMNS[i] for i in indices]
    vals = importances[indices]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(feats[::-1], vals[::-1], color="steelblue")
    ax.set_xlabel("Importance (gain)")
    ax.set_title("Feature Importance — XGBoost")
    plt.tight_layout()

    tmp = Path(f"/tmp/feature_importance_{run_id}.png")
    fig.savefig(str(tmp), dpi=120)
    mlflow.log_artifact(str(tmp), artifact_path="plots")
    plt.close(fig)
    tmp.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Main training loop
# ---------------------------------------------------------------------------

def main(argv: Optional[list] = None) -> None:
    args = parse_args(argv)

    # 1. Configure MLflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(args.experiment_name)
    logger.info(
        "MLflow experiment: '%s'  |  tracking: %s",
        args.experiment_name,
        MLFLOW_TRACKING_URI,
    )

    # 2. Load features
    logger.info(
        "Loading features (league=%s, season=%s)…",
        args.league or "ALL",
        args.season or "ALL",
    )
    fe = FeatureEngineer()
    df = fe.load_features(league_id=args.league, season=args.season)

    if df.empty:
        logger.error("No data returned for the specified filters. Aborting.")
        sys.exit(1)

    logger.info("Loaded %d rows.", len(df))
    X, y = fe.prepare_X_y(df)

    # 3. Temporal train/test split (shuffle=False to respect time ordering)
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    df_train = df.iloc[:split_idx]

    logger.info(
        "Train: %d rows  |  Test: %d rows", len(X_train), len(X_test)
    )

    with mlflow.start_run() as run:
        run_id = run.info.run_id
        mlflow.set_tags(
            {
                "league": str(args.league or "ALL"),
                "season": str(args.season or "ALL"),
                "model_type": "hybrid_poisson_gbm",
            }
        )

        # 4. Train PoissonModel
        logger.info("Training PoissonModel…")
        poisson_model = PoissonModel()
        poisson_model.fit(df_train)

        poisson_path = Path(__file__).parent / "artifacts" / "poisson_model.json"
        poisson_model.save(poisson_path)
        mlflow.log_artifact(str(poisson_path), artifact_path="poisson")

        # 5. Train GradientBoostingModel (with MLflow autolog inside)
        logger.info("Training GradientBoostingModel…")
        gb_model = GradientBoostingModel()
        gb_model.fit(X_train, y_train, eval_set=[(X_test, y_test)])

        # 6. Calibrate & build HybridModel
        logger.info("Calibrating HybridModel…")
        hybrid = HybridModel(
            poisson_model=poisson_model,
            gb_model=gb_model,
            poisson_weight=0.3,
            gb_weight=0.7,
        )
        hybrid.calibrate(X_test, y_test)

        # 7. Evaluate on test set
        logger.info("Evaluating on test set…")
        gb_metrics = gb_model.evaluate(X_test, y_test)

        # Build hybrid predictions for evaluation
        hybrid_preds: list[int] = []
        hybrid_proba_list: list[np.ndarray] = []

        for i in range(len(X_test)):
            row = X_test.iloc[i].to_dict()
            row["home_team_id"] = int(df.iloc[split_idx + i]["home_team_id"])
            row["away_team_id"] = int(df.iloc[split_idx + i]["away_team_id"])
            row["league_id"] = int(df.iloc[split_idx + i].get("league_id", 0))
            row["fixture_id"] = df.iloc[split_idx + i].get("fixture_id")

            pred = hybrid.predict(row)
            from config import TARGET_MAP
            hybrid_preds.append(TARGET_MAP[pred["predicted_result"]])
            hybrid_proba_list.append(
                np.array(
                    [pred["hybrid_home_prob"], pred["hybrid_draw_prob"], pred["hybrid_away_prob"]]
                )
            )

        hybrid_preds_arr = np.array(hybrid_preds)
        hybrid_proba_arr = np.vstack(hybrid_proba_list)

        from sklearn.metrics import accuracy_score, brier_score_loss, log_loss
        hybrid_accuracy = accuracy_score(y_test, hybrid_preds_arr)
        hybrid_logloss = log_loss(y_test, hybrid_proba_arr)
        hybrid_brier = float(
            np.mean(
                [
                    brier_score_loss(
                        (y_test == c).astype(int), hybrid_proba_arr[:, c]
                    )
                    for c in range(3)
                ]
            )
        )

        metrics = {
            "gb_accuracy": gb_metrics["accuracy"],
            "gb_log_loss": gb_metrics["log_loss"],
            "gb_brier_score": gb_metrics["brier_score"],
            "hybrid_accuracy": hybrid_accuracy,
            "hybrid_log_loss": hybrid_logloss,
            "hybrid_brier_score": hybrid_brier,
        }
        mlflow.log_metrics(metrics)
        logger.info("Metrics: %s", metrics)

        # 8. Log artefact plots
        _plot_confusion_matrix(y_test, hybrid_preds_arr, run_id)
        _plot_feature_importance(gb_model, run_id)

        # 9. Register champion if accuracy beats baseline
        if hybrid_accuracy > BASELINE_ACCURACY:
            logger.info(
                "Hybrid accuracy %.4f > baseline %.4f — registering model.",
                hybrid_accuracy,
                BASELINE_ACCURACY,
            )
            mlflow.sklearn.log_model(
                hybrid.gb_model.model,
                artifact_path="champion_model",
                registered_model_name=MODEL_REGISTRY_NAME,
            )
            mlflow.log_param("registered", True)
        else:
            logger.warning(
                "Hybrid accuracy %.4f <= baseline %.4f — model NOT registered.",
                hybrid_accuracy,
                BASELINE_ACCURACY,
            )
            mlflow.log_param("registered", False)

        logger.info("Training complete. MLflow run ID: %s", run_id)


if __name__ == "__main__":
    main()
