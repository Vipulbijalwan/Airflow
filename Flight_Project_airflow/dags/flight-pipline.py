import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import dag, task

AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_ingest import fetch_flight_data
from scripts.silver_transform import transform_bronze_to_silver
from scripts.gold_agg import run_gold_aggregation

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="flights_ops_medallion_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 3, 10),
    schedule="*/30 * * * *",
    catchup=False
)
def flights_ops_medallion_pipeline():

    @task
    def bronze_ingest():
        return fetch_flight_data()

    @task
    def silver_transform(bronze_file):
        return transform_bronze_to_silver(bronze_file)

    @task
    def gold_agg(silver_file):
        return run_gold_aggregation(silver_file)

    bronze_file = bronze_ingest()
    silver_file = silver_transform(bronze_file)
    gold_file = gold_agg(silver_file)


flights_ops_medallion_pipeline()