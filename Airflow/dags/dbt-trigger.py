from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from airflow.sdk import Variable

with DAG(
    "dbt-trigger",
    start_date=datetime(2025, 10, 22),
    schedule=None,
) as dag:
    t1 = DockerOperator(
        task_id="dbt-trigger",
        image="dbt-image:latest",
        auto_remove="force",
        environment={
            "BQ_PROJECT_ID": Variable.get("bq_project_id"),
            "BQ_PROJECT_REGION": Variable.get("bq_project_region"),
        }
    )
