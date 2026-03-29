"""
Hybrid Model.

Blends Poisson and Gradient-Boosting probabilities and applies
isotonic calibration to improve probability sharpness.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.preprocessing import LabelEncoder

from config import (
    FEATURE_COLUMNS,
    INVERSE_TARGET_MAP,
    TARGET_MAP,
)
from gradient_boosting_model import GradientBoostingModel
from poisson_model import PoissonModel

logger = logging.getLogger(__name__)


class HybridModel:
    """
    Weighted ensemble of PoissonModel and GradientBoostingModel.

    Parameters
    ----------
    poisson_weight : float
        Weight applied to Poisson probabilities (default 0.3).
    gb_weight : float
        Weight applied to GBM probabilities (default 0.7).
    """

    def __init__(
        self,
        poisson_model: Optional[PoissonModel] = None,
        gb_model: Optional[GradientBoostingModel] = None,
        poisson_weight: float = 0.3,
        gb_weight: float = 0.7,
    ) -> None:
        if abs(poisson_weight + gb_weight - 1.0) > 1e-6:
            raise ValueError("poisson_weight + gb_weight must equal 1.0")

        self.poisson_weight = poisson_weight
        self.gb_weight = gb_weight
        self.poisson_model: Optional[PoissonModel] = poisson_model
        self.gb_model: Optional[GradientBoostingModel] = gb_model
        self._calibrated_gb: Optional[Any] = None
        self._is_calibrated: bool = False

    # ------------------------------------------------------------------
    # Calibration
    # ------------------------------------------------------------------

    def calibrate(
        self,
        X_val: pd.DataFrame,
        y_val: pd.Series,
    ) -> "HybridModel":
        """
        Apply isotonic regression calibration to the GB model output.

        Parameters
        ----------
        X_val : pd.DataFrame
            Validation features.
        y_val : pd.Series
            Integer-encoded ground-truth labels.

        Returns
        -------
        self
        """
        if self.gb_model is None:
            raise RuntimeError("gb_model must be set before calibration.")

        self._calibrated_gb = CalibratedClassifierCV(
            self.gb_model.model,
            cv="prefit",
            method="isotonic",
        )
        self._calibrated_gb.fit(X_val, y_val)
        self._is_calibrated = True
        logger.info("HybridModel: isotonic calibration applied on %d samples.", len(X_val))
        return self

    # ------------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------------

    def predict_proba_from_array(
        self,
        gb_proba: np.ndarray,
        poisson_proba: Optional[np.ndarray] = None,
    ) -> np.ndarray:
        """
        Blend pre-computed probability arrays.

        Parameters
        ----------
        gb_proba : np.ndarray, shape (n, 3)
        poisson_proba : np.ndarray, shape (n, 3), optional
            When omitted, only GB probabilities are used (weight 1.0).

        Returns
        -------
        np.ndarray, shape (n, 3)  — [home_prob, draw_prob, away_prob]
        """
        if poisson_proba is None:
            return gb_proba

        hybrid = (
            self.poisson_weight * poisson_proba
            + self.gb_weight * gb_proba
        )
        # Re-normalise row-wise
        row_sums = hybrid.sum(axis=1, keepdims=True)
        row_sums = np.where(row_sums == 0, 1.0, row_sums)
        return hybrid / row_sums

    def predict(self, fixture_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a full prediction payload for a single fixture.

        Parameters
        ----------
        fixture_data : dict
            Must contain at minimum:
            - ``fixture_id``
            - ``home_team_id``
            - ``away_team_id``
            - ``league_id``
            - All FEATURE_COLUMNS (may be NaN for missing features)

        Returns
        -------
        dict with probability scores, predicted result and confidence.
        """
        fixture_id = fixture_data.get("fixture_id")
        home_team_id = fixture_data["home_team_id"]
        away_team_id = fixture_data["away_team_id"]
        league_id = fixture_data.get("league_id")

        # --- Poisson probabilities ---
        poisson_probs: Dict[str, float] = {
            "home_prob": 1 / 3,
            "draw_prob": 1 / 3,
            "away_prob": 1 / 3,
        }
        expected_home_goals: Optional[float] = None
        expected_away_goals: Optional[float] = None

        if self.poisson_model is not None and self.poisson_model._is_fitted:
            try:
                poisson_probs = self.poisson_model.predict_proba(
                    home_team_id, away_team_id, league_id
                )
                expected_home_goals, expected_away_goals = (
                    self.poisson_model.predict_goals(
                        home_team_id, away_team_id, league_id
                    )
                )
            except Exception as exc:
                logger.warning(
                    "Poisson prediction failed for fixture %s: %s", fixture_id, exc
                )

        # --- GBM probabilities ---
        gb_probs_arr = np.array([[1 / 3, 1 / 3, 1 / 3]])

        if self.gb_model is not None and self.gb_model._is_fitted:
            feat_row = {col: fixture_data.get(col, np.nan) for col in FEATURE_COLUMNS}
            X_row = pd.DataFrame([feat_row])

            try:
                if self._is_calibrated and self._calibrated_gb is not None:
                    gb_probs_arr = self._calibrated_gb.predict_proba(X_row)
                else:
                    gb_probs_arr = self.gb_model.predict_proba(X_row)
            except Exception as exc:
                logger.warning(
                    "GBM prediction failed for fixture %s: %s", fixture_id, exc
                )

        poisson_arr = np.array(
            [[poisson_probs["home_prob"], poisson_probs["draw_prob"], poisson_probs["away_prob"]]]
        )

        hybrid_arr = self.predict_proba_from_array(gb_probs_arr, poisson_arr)
        hybrid_probs = hybrid_arr[0]

        predicted_class = int(np.argmax(hybrid_probs))
        predicted_result = INVERSE_TARGET_MAP[predicted_class]
        confidence_score = float(np.max(hybrid_probs))

        return {
            "fixture_id": fixture_id,
            "league_id": league_id,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
            # Poisson
            "poisson_home_prob": float(poisson_probs["home_prob"]),
            "poisson_draw_prob": float(poisson_probs["draw_prob"]),
            "poisson_away_prob": float(poisson_probs["away_prob"]),
            # GBM
            "gb_home_prob": float(gb_probs_arr[0][0]),
            "gb_draw_prob": float(gb_probs_arr[0][1]),
            "gb_away_prob": float(gb_probs_arr[0][2]),
            # Hybrid
            "hybrid_home_prob": float(hybrid_probs[0]),
            "hybrid_draw_prob": float(hybrid_probs[1]),
            "hybrid_away_prob": float(hybrid_probs[2]),
            # Summary
            "predicted_result": predicted_result,
            "confidence_score": confidence_score,
            "expected_home_goals": expected_home_goals,
            "expected_away_goals": expected_away_goals,
        }
