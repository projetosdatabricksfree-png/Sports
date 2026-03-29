"""
DAG — dbt Pipeline: Staging → Intermediate → Marts
Orquestra o build completo do projeto dbt de forma incremental.
Executa diariamente. Cada camada é um TaskGroup independente.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/dbt_project"
DBT_PROFILES_DIR = "/opt/dbt_project"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dbt_pipeline",
    description="dbt full pipeline: staging → intermediate → marts",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "data-platform", "production"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt source freshness --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_build_staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt build --select staging.* --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_build_intermediate = BashOperator(
        task_id="dbt_build_intermediate",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt build --select intermediate.* --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_build_marts = BashOperator(
        task_id="dbt_build_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt build --select marts.* --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test_all",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir {DBT_PROFILES_DIR}",
    )

    (
        dbt_deps
        >> dbt_source_freshness
        >> dbt_build_staging
        >> dbt_build_intermediate
        >> dbt_build_marts
        >> dbt_test
        >> dbt_docs_generate
    )
