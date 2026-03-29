"""
Poisson Goal Model.

Estimates team attack / defence strengths via Maximum-Likelihood Estimation
(Dixon-Coles style) and uses the bivariate-independent Poisson distribution
to derive match-outcome probabilities.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.stats import poisson

logger = logging.getLogger(__name__)

# Maximum score considered when building the probability matrix
MAX_GOALS = 7  # 0..6


class PoissonModel:
    """
    Bivariate independent Poisson model for football score prediction.

    Attack / defence parameters are fitted per team using negative
    log-likelihood minimisation (MLE). A home-advantage intercept and
    a global average are also estimated.
    """

    def __init__(self) -> None:
        self._attack: Dict[int, float] = {}
        self._defence: Dict[int, float] = {}
        self._home_advantage: float = 1.0
        self._avg_goals: float = 1.35
        self._teams: list[int] = []
        self._is_fitted: bool = False

    # ------------------------------------------------------------------
    # Fitting
    # ------------------------------------------------------------------

    def fit(self, df: pd.DataFrame) -> "PoissonModel":
        """
        Fit attack / defence strengths from historical match data.

        Parameters
        ----------
        df:
            DataFrame that must contain columns:
            ``home_team_id``, ``away_team_id``,
            ``home_goals``, ``away_goals``.
            Rows with NaN in those columns are ignored.
        """
        required = ["home_team_id", "away_team_id", "home_goals", "away_goals"]
        df = df.dropna(subset=required).copy()
        df["home_goals"] = df["home_goals"].astype(int)
        df["away_goals"] = df["away_goals"].astype(int)

        teams = sorted(
            set(df["home_team_id"].tolist() + df["away_team_id"].tolist())
        )
        self._teams = teams
        team_index = {t: i for i, t in enumerate(teams)}
        n = len(teams)

        # Initial parameters: [attack_0..n-1, defence_0..n-1, home_adv, intercept]
        x0 = np.concatenate(
            [np.ones(n), np.ones(n), np.array([0.25, np.log(1.35)])]
        )

        def neg_log_likelihood(params: np.ndarray) -> float:
            attack = params[:n]
            defence = params[n : 2 * n]
            home_adv = params[2 * n]
            intercept = params[2 * n + 1]

            log_like = 0.0
            for _, row in df.iterrows():
                hi = team_index[row["home_team_id"]]
                ai = team_index[row["away_team_id"]]

                mu_home = np.exp(intercept + home_adv + attack[hi] - defence[ai])
                mu_away = np.exp(intercept + attack[ai] - defence[hi])

                log_like += poisson.logpmf(row["home_goals"], mu_home)
                log_like += poisson.logpmf(row["away_goals"], mu_away)

            return -log_like

        # Sum-to-zero constraint on attack parameters for identifiability
        constraints = [
            {"type": "eq", "fun": lambda p: np.sum(p[:n])},
        ]

        result = minimize(
            neg_log_likelihood,
            x0,
            method="SLSQP",
            constraints=constraints,
            options={"maxiter": 200, "ftol": 1e-8},
        )

        if not result.success:
            logger.warning(
                "PoissonModel MLE did not converge: %s — falling back to "
                "average-goals parametrisation.",
                result.message,
            )
            self._fit_fallback(df)
            return self

        params = result.x
        attack = params[:n]
        defence = params[n : 2 * n]
        self._home_advantage = params[2 * n]
        self._avg_goals = params[2 * n + 1]

        self._attack = {t: float(attack[i]) for i, t in enumerate(teams)}
        self._defence = {t: float(defence[i]) for i, t in enumerate(teams)}
        self._is_fitted = True

        logger.info(
            "PoissonModel fitted on %d matches, %d teams.", len(df), n
        )
        return self

    def _fit_fallback(self, df: pd.DataFrame) -> None:
        """
        Simple average-goals fallback when MLE fails to converge.
        Each team's attack = avg goals scored / league avg.
        Each team's defence = avg goals conceded / league avg.
        """
        league_avg = (
            df["home_goals"].mean() + df["away_goals"].mean()
        ) / 2
        league_avg = max(league_avg, 0.5)

        teams = self._teams

        for team in teams:
            home_mask = df["home_team_id"] == team
            away_mask = df["away_team_id"] == team

            goals_scored = pd.concat(
                [df.loc[home_mask, "home_goals"], df.loc[away_mask, "away_goals"]]
            ).mean()
            goals_conceded = pd.concat(
                [df.loc[home_mask, "away_goals"], df.loc[away_mask, "home_goals"]]
            ).mean()

            self._attack[team] = float(
                np.log(max(goals_scored, 0.1) / league_avg)
            )
            self._defence[team] = float(
                np.log(max(goals_conceded, 0.1) / league_avg)
            )

        self._home_advantage = 0.25
        self._avg_goals = np.log(league_avg)
        self._is_fitted = True

    # ------------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------------

    def _expected_goals(
        self,
        home_team_id: int,
        away_team_id: int,
    ) -> Tuple[float, float]:
        """Return (mu_home, mu_away) expected goals."""
        ha = self._attack.get(home_team_id, 0.0)
        hd = self._defence.get(home_team_id, 0.0)
        aa = self._attack.get(away_team_id, 0.0)
        ad = self._defence.get(away_team_id, 0.0)

        mu_home = np.exp(self._avg_goals + self._home_advantage + ha - ad)
        mu_away = np.exp(self._avg_goals + aa - hd)
        return float(mu_home), float(mu_away)

    def predict_goals(
        self,
        home_team_id: int,
        away_team_id: int,
        league_id: Optional[int] = None,  # kept for API symmetry
    ) -> Tuple[float, float]:
        """
        Return expected goals (home, away) for a fixture.

        Parameters
        ----------
        home_team_id, away_team_id:
            Team identifiers used during :meth:`fit`.
        league_id:
            Unused at this level; accepted for interface compatibility.

        Returns
        -------
        (expected_home_goals, expected_away_goals)
        """
        self._assert_fitted()
        return self._expected_goals(home_team_id, away_team_id)

    def predict_proba(
        self,
        home_team_id: int,
        away_team_id: int,
        league_id: Optional[int] = None,
    ) -> Dict[str, float]:
        """
        Return outcome probabilities derived from the Poisson score matrix.

        Returns
        -------
        dict with keys ``home_prob``, ``draw_prob``, ``away_prob``.
        """
        self._assert_fitted()
        mu_home, mu_away = self._expected_goals(home_team_id, away_team_id)

        # Build score probability matrix (MAX_GOALS x MAX_GOALS)
        home_pmf = poisson.pmf(np.arange(MAX_GOALS), mu_home)
        away_pmf = poisson.pmf(np.arange(MAX_GOALS), mu_away)
        score_matrix = np.outer(home_pmf, away_pmf)

        home_prob = float(np.sum(np.tril(score_matrix, -1)))
        draw_prob = float(np.sum(np.diag(score_matrix)))
        away_prob = float(np.sum(np.triu(score_matrix, 1)))

        # Normalise in case of rounding
        total = home_prob + draw_prob + away_prob
        return {
            "home_prob": home_prob / total,
            "draw_prob": draw_prob / total,
            "away_prob": away_prob / total,
        }

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: str | Path) -> None:
        """Persist model parameters to a JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "attack": {str(k): v for k, v in self._attack.items()},
            "defence": {str(k): v for k, v in self._defence.items()},
            "home_advantage": self._home_advantage,
            "avg_goals": self._avg_goals,
            "teams": [str(t) for t in self._teams],
        }
        path.write_text(json.dumps(payload, indent=2))
        logger.info("PoissonModel saved to %s", path)

    @classmethod
    def load(cls, path: str | Path) -> "PoissonModel":
        """Restore a model previously saved with :meth:`save`."""
        path = Path(path)
        payload = json.loads(path.read_text())
        model = cls()
        model._attack = {int(k): v for k, v in payload["attack"].items()}
        model._defence = {int(k): v for k, v in payload["defence"].items()}
        model._home_advantage = payload["home_advantage"]
        model._avg_goals = payload["avg_goals"]
        model._teams = [int(t) for t in payload["teams"]]
        model._is_fitted = True
        logger.info("PoissonModel loaded from %s", path)
        return model

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _assert_fitted(self) -> None:
        if not self._is_fitted:
            raise RuntimeError(
                "PoissonModel is not fitted. Call fit() before predict_*()."
            )
