import os

# Database
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://airflow:airflow@localhost:5432/airflow",
)

# MLflow
MLFLOW_TRACKING_URI = "http://localhost:5000"
MLFLOW_EXPERIMENT_NAME = "football_predictions"
MODEL_REGISTRY_NAME = "football_champion"

# Supported leagues (API-Football / similar IDs)
LEAGUES = [
    39,   # Premier League
    140,  # La Liga
    135,  # Serie A
    78,   # Bundesliga
    61,   # Ligue 1
    94,   # Primeira Liga
    88,   # Eredivisie
    203,  # Super Lig
]

# Features used by ML models — must match fct_match_features columns
FEATURE_COLUMNS = [
    "home_avg_points_l5",
    "home_avg_goals_scored_l5",
    "home_avg_goals_conceded_l5",
    "home_win_rate_l5",
    "home_avg_points_l10",
    "away_avg_points_l5",
    "away_avg_goals_scored_l5",
    "away_avg_goals_conceded_l5",
    "away_win_rate_l5",
    "away_avg_points_l10",
    "h2h_home_wins",
    "h2h_draws",
    "h2h_avg_goals",
    "home_implicit_prob",
    "draw_implicit_prob",
    "away_implicit_prob",
]

TARGET_COLUMN = "match_result"

TARGET_MAP = {
    "HOME_WIN": 0,
    "DRAW": 1,
    "AWAY_WIN": 2,
}

INVERSE_TARGET_MAP = {v: k for k, v in TARGET_MAP.items()}

# Baseline accuracy threshold to promote a model to the registry
BASELINE_ACCURACY = 0.50

# Default prediction horizon
DEFAULT_DAYS_AHEAD = 7
