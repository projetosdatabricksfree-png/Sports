"""
Gradient Boosting Model (XGBoost) for football result classification.

Trains an XGBClassifier with 5-fold cross-validation, logs experiments
with MLflow autolog and exposes calibrated probabilities.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss
from sklearn.model_selection import StratifiedKFold, cross_val_score
from xgboost import XGBClassifier

from config import FEATURE_COLUMNS

logger = logging.getLogger(__name__)

_DEFAULT_PARAMS = dict(
    n_estimators=300,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    min_child_weight=3,
    gamma=0.1,
    reg_alpha=0.1,
    reg_lambda=1.0,
    objective="multi:softprob",
    num_class=3,
    eval_metric="mlogloss",
    use_label_encoder=False,
    random_state=42,
    n_jobs=-1,
)


class GradientBoostingModel:
    """
    XGBoost multi-class classifier (HOME_WIN / DRAW / AWAY_WIN).

    Usage
    -----
    >>> model = GradientBoostingModel()
    >>> model.fit(X_train, y_train)
    >>> proba = model.predict_proba(X_test)
    >>> metrics = model.evaluate(X_test, y_test)
    """

    def __init__(self, params: Optional[Dict] = None) -> None:
        merged = {**_DEFAULT_PARAMS, **(params or {})}
        self._model = XGBClassifier(**merged)
        self._is_fitted: bool = False
        self._cv_scores: Optional[np.ndarray] = None
        self.feature_names_: list[str] = FEATURE_COLUMNS

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def fit(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        eval_set: Optional[list] = None,
    ) -> "GradientBoostingModel":
        """
        Train the XGBClassifier and run 5-fold CV for performance estimation.

        Parameters
        ----------
        X : pd.DataFrame
            Feature matrix (FEATURE_COLUMNS).
        y : pd.Series
            Integer-encoded target (0=HOME_WIN, 1=DRAW, 2=AWAY_WIN).
        eval_set : list, optional
            Passed to XGBoost's ``fit`` as validation set for early stopping.

        Returns
        -------
        self
        """
        mlflow.xgboost.autolog(log_models=False, silent=True)

        # 5-fold cross-validation (accuracy)
        cv = StratifiedKFold(n_splits=5, shuffle=False)
        self._cv_scores = cross_val_score(
            XGBClassifier(**_DEFAULT_PARAMS),
            X,
            y,
            cv=cv,
            scoring="accuracy",
            n_jobs=-1,
        )
        logger.info(
            "CV accuracy: %.4f +/- %.4f",
            self._cv_scores.mean(),
            self._cv_scores.std(),
        )
        mlflow.log_metric("cv_accuracy_mean", float(self._cv_scores.mean()))
        mlflow.log_metric("cv_accuracy_std", float(self._cv_scores.std()))

        fit_kwargs: Dict = {}
        if eval_set is not None:
            fit_kwargs["eval_set"] = eval_set
            fit_kwargs["verbose"] = False

        self._model.fit(X, y, **fit_kwargs)
        self._is_fitted = True

        # Log feature importances
        self._log_feature_importance()

        logger.info("GradientBoostingModel fitted on %d samples.", len(X))
        return self

    # ------------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------------

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """
        Return class probabilities.

        Parameters
        ----------
        X : pd.DataFrame
            Feature matrix (same columns used during fit).

        Returns
        -------
        np.ndarray of shape (n_samples, 3)
            Columns: [home_prob, draw_prob, away_prob]
        """
        self._assert_fitted()
        return self._model.predict_proba(X)

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Return predicted class labels (0 / 1 / 2)."""
        self._assert_fitted()
        return self._model.predict(X)

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------

    def evaluate(
        self,
        X_test: pd.DataFrame,
        y_test: pd.Series,
    ) -> Dict[str, float]:
        """
        Compute accuracy, log-loss and Brier score on a held-out set.

        Returns
        -------
        dict with keys ``accuracy``, ``log_loss``, ``brier_score``.
        """
        self._assert_fitted()
        proba = self.predict_proba(X_test)
        preds = np.argmax(proba, axis=1)

        acc = accuracy_score(y_test, preds)
        ll = log_loss(y_test, proba)

        # Average Brier score across classes (one-vs-rest)
        n_classes = proba.shape[1]
        brier = float(
            np.mean(
                [
                    brier_score_loss(
                        (y_test == c).astype(int), proba[:, c]
                    )
                    for c in range(n_classes)
                ]
            )
        )

        metrics = {
            "accuracy": float(acc),
            "log_loss": float(ll),
            "brier_score": brier,
        }
        logger.info("GBM test metrics: %s", metrics)
        return metrics

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def get_booster(self):
        """Return the underlying XGBoost Booster for MLflow logging."""
        self._assert_fitted()
        return self._model.get_booster()

    @property
    def model(self) -> XGBClassifier:
        return self._model

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _log_feature_importance(self) -> None:
        """Log feature importance scores to MLflow."""
        try:
            importance = self._model.feature_importances_
            for feat, imp in zip(self.feature_names_, importance):
                mlflow.log_metric(f"fi_{feat}", float(imp))
        except Exception as exc:
            logger.warning("Could not log feature importances: %s", exc)

    def _assert_fitted(self) -> None:
        if not self._is_fitted:
            raise RuntimeError(
                "GradientBoostingModel is not fitted. Call fit() first."
            )
