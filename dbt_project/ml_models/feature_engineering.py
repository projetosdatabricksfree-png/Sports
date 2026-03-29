"""
Feature Engineering module.

Loads match features from the dbt mart layer and prepares arrays
ready for model training and inference.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional, Tuple

import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sqlalchemy import create_engine, text

from config import DATABASE_URL, FEATURE_COLUMNS, TARGET_COLUMN, TARGET_MAP


class FeatureEngineer:
    """Loads and prepares features from marts.fct_match_features."""

    def __init__(self) -> None:
        self._engine = create_engine(DATABASE_URL)
        self._label_encoder = LabelEncoder()
        self._label_encoder.classes_ = list(TARGET_MAP.keys())

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_features(
        self,
        league_id: Optional[int] = None,
        season: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Load match feature rows from marts.fct_match_features.

        Parameters
        ----------
        league_id:
            When provided, filters to a single league.
        season:
            When provided, filters to a specific season year (e.g. 2023).

        Returns
        -------
        pd.DataFrame with all columns from fct_match_features, ordered
        chronologically by match_date.
        """
        conditions: list[str] = ["match_result IS NOT NULL"]
        params: dict = {}

        if league_id is not None:
            conditions.append("league_id = :league_id")
            params["league_id"] = league_id

        if season is not None:
            conditions.append("season = :season")
            params["season"] = season

        where_clause = " AND ".join(conditions)
        query = text(
            f"""
            SELECT *
            FROM   marts.fct_match_features
            WHERE  {where_clause}
            ORDER  BY match_date ASC
            """
        )

        with self._engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params)

        df = df.dropna(subset=FEATURE_COLUMNS + [TARGET_COLUMN])
        return df

    def prepare_X_y(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Split *df* into feature matrix X and encoded target vector y.

        Parameters
        ----------
        df:
            DataFrame returned by :meth:`load_features`.

        Returns
        -------
        X : pd.DataFrame
            Subset of *df* containing only FEATURE_COLUMNS, with NaN rows
            dropped and index reset.
        y : pd.Series
            Integer-encoded target (HOME_WIN=0, DRAW=1, AWAY_WIN=2).
        """
        df = df.dropna(subset=FEATURE_COLUMNS + [TARGET_COLUMN]).reset_index(
            drop=True
        )

        X = df[FEATURE_COLUMNS].copy()
        raw_target = df[TARGET_COLUMN]

        y = raw_target.map(TARGET_MAP).astype(int)

        return X, y

    def get_upcoming_fixtures(
        self, days_ahead: int = 7
    ) -> pd.DataFrame:
        """
        Return upcoming fixtures for the next *days_ahead* days.

        Pulls from marts.fct_match_features (rows where match_result IS NULL
        and match_date is in the future), plus any dedicated fixture/staging
        table if available.

        Returns
        -------
        pd.DataFrame with columns including fixture_id, league_id,
        match_date, home_team_id, away_team_id and all FEATURE_COLUMNS
        (which may contain NaNs for very recent fixtures).
        """
        today = date.today()
        cutoff = today + timedelta(days=days_ahead)

        query = text(
            """
            SELECT *
            FROM   marts.fct_match_features
            WHERE  match_result IS NULL
              AND  match_date BETWEEN :today AND :cutoff
            ORDER  BY match_date ASC
            """
        )

        with self._engine.connect() as conn:
            df = pd.read_sql(
                query,
                conn,
                params={"today": today.isoformat(), "cutoff": cutoff.isoformat()},
            )

        return df

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @property
    def engine(self):
        return self._engine
