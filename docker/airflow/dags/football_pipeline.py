"""
DAG — Football Prediction Pipeline (Full)
Orquestra ingestão da API-Football, processamento Spark, transformações dbt,
treinamento de modelos ML e geração de previsões para todas as Séries A.

Pipeline: ingest_data → spark_processing → dbt_run → dbt_test → train_model → predictions → superset_refresh
Execução: Diária às 04:00 UTC (antes dos jogos do dia)
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# =============================================================================
# Configurações
# =============================================================================
DBT_PROJECT_DIR = "/opt/dbt_project"
DBT_PROFILES_DIR = "/opt/dbt_project"
INGESTION_DIR = "/opt/ingestion"
ML_DIR = "/opt/dbt_project/ml_models"

LEAGUES = {
    "brasileirao": 71,
    "premier_league": 39,
    "la_liga": 140,
    "serie_a_italy": 135,
    "bundesliga": 78,
    "ligue_1": 61,
    "eredivisie": 88,
    "primeira_liga": 94,
    "liga_profesional": 128,
    "liga_mx": 262,
    "mls": 253,
    "champions_league": 2,
    "europa_league": 3,
    "libertadores": 13,
    "sulamericana": 11,
}

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "execution_timeout": timedelta(hours=2),
}

# =============================================================================
# DAG
# =============================================================================
with DAG(
    dag_id="football_prediction_pipeline",
    description="Pipeline completo: API-Football → Spark → dbt → ML → Predictions",
    schedule="0 4 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["football", "predictions", "ml", "production"],
    doc_md="""
    ## Football Prediction Pipeline

    Pipeline diário de previsão de futebol cobrindo todas as Séries A nacionais e internacionais.

    ### Fluxo
    1. **Ingestão** — Coleta fixtures, standings, teams e odds da API-Football
    2. **Spark** — Processamento e feature engineering distribuído
    3. **dbt** — Transformações em camadas (staging → intermediate → marts)
    4. **ML Training** — Treina Poisson + Gradient Boosting + Hybrid model
    5. **Predictions** — Gera previsões para partidas dos próximos 7 dias
    6. **Superset** — Refresh automático dos dashboards

    ### Ligas cobertas
    Brasileirão, Premier League, La Liga, Serie A, Bundesliga, Ligue 1,
    Eredivisie, Primeira Liga, Liga Profesional, Liga MX, MLS,
    Champions League, Europa League, Libertadores, Sulamericana
    """,
) as dag:

    # =========================================================================
    # FASE 1 — Ingestão da API-Football
    # =========================================================================
    with TaskGroup("ingestion", tooltip="Coleta dados da API-Football") as ingestion_group:

        ingest_fixtures = BashOperator(
            task_id="ingest_fixtures",
            bash_command=f"cd {INGESTION_DIR} && python ingest_fixtures.py",
            env={"API_FOOTBALL_KEY": "9f1290452cb4f3790301e23b6786f897"},
        )

        ingest_standings = BashOperator(
            task_id="ingest_standings",
            bash_command=f"cd {INGESTION_DIR} && python ingest_standings.py",
            env={"API_FOOTBALL_KEY": "9f1290452cb4f3790301e23b6786f897"},
        )

        ingest_teams = BashOperator(
            task_id="ingest_teams",
            bash_command=f"cd {INGESTION_DIR} && python ingest_teams.py",
            env={"API_FOOTBALL_KEY": "9f1290452cb4f3790301e23b6786f897"},
        )

        ingest_odds = BashOperator(
            task_id="ingest_odds",
            bash_command=f"cd {INGESTION_DIR} && python ingest_odds.py",
            env={"API_FOOTBALL_KEY": "9f1290452cb4f3790301e23b6786f897"},
        )

        # Fixtures e Teams em paralelo, depois Standings e Odds
        [ingest_fixtures, ingest_teams] >> ingest_standings
        ingest_fixtures >> ingest_odds

    # =========================================================================
    # FASE 2 — Processamento Spark (Feature Engineering distribuído)
    # =========================================================================
    with TaskGroup("spark_processing", tooltip="Feature engineering com Spark") as spark_group:

        spark_team_form = BashOperator(
            task_id="spark_team_form",
            bash_command=(
                "docker exec spark-master spark-submit "
                "--master spark://spark-master:7077 "
                "--conf spark.sql.shuffle.partitions=8 "
                "/opt/spark/jobs/compute_team_form.py"
            ),
        )

        spark_h2h = BashOperator(
            task_id="spark_head_to_head",
            bash_command=(
                "docker exec spark-master spark-submit "
                "--master spark://spark-master:7077 "
                "/opt/spark/jobs/compute_h2h.py"
            ),
        )

        [spark_team_form, spark_h2h]

    # =========================================================================
    # FASE 3 — dbt Transformações
    # =========================================================================
    with TaskGroup("dbt_pipeline", tooltip="Transformações dbt em camadas") as dbt_group:

        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"docker exec dbt dbt deps --profiles-dir {DBT_PROFILES_DIR}",
        )

        dbt_source_freshness = BashOperator(
            task_id="dbt_source_freshness",
            bash_command=f"docker exec dbt dbt source freshness --profiles-dir {DBT_PROFILES_DIR}",
        )

        dbt_staging = BashOperator(
            task_id="dbt_build_staging",
            bash_command=(
                f"docker exec dbt dbt build "
                f"--select staging.* --profiles-dir {DBT_PROFILES_DIR}"
            ),
        )

        dbt_intermediate = BashOperator(
            task_id="dbt_build_intermediate",
            bash_command=(
                f"docker exec dbt dbt build "
                f"--select intermediate.* --profiles-dir {DBT_PROFILES_DIR}"
            ),
        )

        dbt_marts = BashOperator(
            task_id="dbt_build_marts",
            bash_command=(
                f"docker exec dbt dbt build "
                f"--select marts.* --profiles-dir {DBT_PROFILES_DIR}"
            ),
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"docker exec dbt dbt test --profiles-dir {DBT_PROFILES_DIR}",
        )

        dbt_docs = BashOperator(
            task_id="dbt_docs_generate",
            bash_command=f"docker exec dbt dbt docs generate --profiles-dir {DBT_PROFILES_DIR}",
        )

        dbt_deps >> dbt_source_freshness >> dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_test >> dbt_docs

    # =========================================================================
    # FASE 4 — Treinamento de Modelos ML
    # =========================================================================
    with TaskGroup("ml_training", tooltip="Treina modelos Poisson + GB + Hybrid") as ml_group:

        train_all_leagues = BashOperator(
            task_id="train_models",
            bash_command=(
                f"docker exec dbt python {ML_DIR}/train.py "
                f"--season 2025 "
                f"--experiment-name football_predictions"
            ),
        )

        evaluate_models = BashOperator(
            task_id="evaluate_models",
            bash_command=f"docker exec dbt python {ML_DIR}/evaluate.py",
        )

        train_all_leagues >> evaluate_models

    # =========================================================================
    # FASE 5 — Geração de Previsões
    # =========================================================================
    with TaskGroup("predictions", tooltip="Gera previsões para os próximos 7 dias") as pred_group:

        generate_predictions = BashOperator(
            task_id="generate_predictions",
            bash_command=f"docker exec dbt python {ML_DIR}/predict.py",
        )

        validate_predictions = BashOperator(
            task_id="validate_predictions",
            bash_command=(
                f"docker exec dbt dbt test "
                f"--select marts.fct_predictions "
                f"--profiles-dir {DBT_PROFILES_DIR}"
            ),
        )

        generate_predictions >> validate_predictions

    # =========================================================================
    # FASE 6 — Refresh Superset
    # =========================================================================
    superset_refresh = BashOperator(
        task_id="superset_refresh_dashboards",
        bash_command=(
            "curl -s -X POST http://superset:8088/api/v1/cache/invalidate "
            "-H 'Content-Type: application/json' "
            "-H 'Authorization: Bearer $(curl -s -X POST http://superset:8088/api/v1/security/login "
            "-d \\'{\\"username\\":\\"admin\\",\\"password\\":\\"admin\\",\\"provider\\":\\"db\\"}\\'  "
            "-H \\'Content-Type: application/json\\' | python3 -c \\'import sys,json; print(json.load(sys.stdin)[\"access_token\"])\\')"
            "' -d '{\"filter\": {}}' || echo 'Superset refresh tentado'"
        ),
    )

    # =========================================================================
    # Dependências entre grupos
    # =========================================================================
    ingestion_group >> spark_group >> dbt_group >> ml_group >> pred_group >> superset_refresh
