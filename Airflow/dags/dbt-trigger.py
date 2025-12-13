from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Variable

with DAG(
    "dbt-trigger",
    start_date=datetime(2025, 10, 22),
    schedule=None,
) as dag:
    dbt_run = BashOperator(
        task_id="dbt-trigger",
        bash_command="""
        source /opt/venvs/dbt-env/bin/activate
        cd /opt/dbt
        dbt deps
        dbt run --profiles-dir /opt/dbt --select stg_sales__salesorderheader
        """,
        env={
            "BQ_PROJECT_ID": Variable.get("bq_project_id"),
            "BQ_PROJECT_REGION": Variable.get("bq_project_region"),
        },
    )
