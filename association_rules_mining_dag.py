from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from google.cloud import bigquery
import logging
import os

default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=10)
}


@task(retries=1)
def get_territories(dag_run=None):
    assert dag_run

    conf = dag_run.conf
    project_id = conf.get("project_id", None)
    dataset = conf.get("dataset", None)
    assert project_id is not None, "Project ID not provided"
    assert dataset is not None, "Dataset not provided"

    client = bigquery.Client(project=project_id)
    query = f"""SELECT DISTINCT TerritoryID
            FROM `{project_id}.{dataset}.dim_territory`"""
    result = client.query(query).to_dataframe()

    ret = result['TerritoryID'].to_list()
    ret.append(None)
    logging.info(f"Found the following TerritoryIDs: {ret}")

    return ret


@task(pool="fpgrowth_pool", pool_slots=1)
def run_fpgrowth(territory_id, dag_run=None):
    assert dag_run
    import subprocess

    conf = dag_run.conf
    project_id = conf.get("project_id", None)
    dataset = conf.get("dataset", None)
    assert project_id is not None, "Project ID not provided"
    assert dataset is not None, "Dataset not provided"

    region = str(conf.get("region", "us-west1"))
    date_filter = conf.get("date_filter", None)

    # Set up default info and service account credentials
    key_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    subprocess.run(["gcloud", "auth", "activate-service-account",
                   f"--key-file={key_file}"], check=True)
    subprocess.run(["gcloud", "config", "set", "account",
                   "airflow@argon-triode-474919-k7.iam.gserviceaccount.com"],
                   check=True)
    subprocess.run(["gcloud", "config", "set",
                   "project", project_id], check=True)

    args = [project_id, dataset]
    if territory_id is not None:
        args.append(f"--territory_id={territory_id}")
    if date_filter is not None:
        args.append(f"--date_filter={date_filter}")

    cmd = [
        "gcloud", "run", "jobs", "execute", "fpgrowth-job",
        f"--region={region}",
        f"--args={','.join(args)}"
    ]

    subprocess.run(cmd, check=True)
    return f"Job for {territory_id} done."


with DAG(
    "association_rules_mining",
    max_active_runs=1,
    start_date=datetime(2025, 10, 22),
    schedule=None,
    default_args=default_args,
    catchup=False
) as dag:
    territories = get_territories()
    run_fpgrowth.expand(territory_id=territories)
