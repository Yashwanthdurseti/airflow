from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append("/opt/airflow")
from src.data.ingest_data import ingest_data

default_args = {
    "owner": "mlops",
    "retries": 1,
}

with DAG(
    dag_id="ml_training_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["mlops", "training", "production"],
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("Pipeline started"),
    )

    ingest = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data,
        provide_context=True,
    )

    end = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Pipeline finished"),
    )

    start >> ingest >> end
