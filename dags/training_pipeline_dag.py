from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append("/opt/airflow")
from src.data.ingest_data import ingest_data
from src.data.validate_data import validate_data
from src.features.build_features import build_features
from src.training.train_model import train_model
from src.evaluation.select_best_model import select_best_model
from src.evaluation.register_model import register_best_model

default_args = {
    
    "owner": "mlops",
    "retries": 1,
}
MODEL_LIST = ["logistic", "random_forest", "xgboost", "lightgbm"]


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

    validate = PythonOperator(
        task_id = "validate_data",
        python_callable = validate_data,
        provide_context = True
    )

    features = PythonOperator(
        task_id = "build_features",
        python_callable = build_features,
        provide_context = True
    )


    train_tasks = []
    for model_name in MODEL_LIST:
        task = PythonOperator(
            task_id = f"train_{model_name}",
            python_callable = train_model,
            op_kwargs = {"model_name":model_name},
            provide_context = True
        )
        train_tasks.append(task)

    select_model = PythonOperator(
        
    task_id="select_best_model",
    python_callable=select_best_model,
    provide_context=True,
        
    )

    register = PythonOperator(
        
    task_id="register_best_model",
    python_callable=register_best_model,
    provide_context=True,
        
    )



    end = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Pipeline finished"),
    )

    start >> ingest >> validate >> features 
    features >> train_tasks >> select_model >> register >> end
