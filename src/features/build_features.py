import os
import pandas as pd
import logging
def build_features(**context):
    ti = context["ti"]
    data_path = ti.xcom_pull(task_ids = "ingest_data")
    if not data_path or not os.path.exists(data_path):
        raise ValueError("Raw data not found for feature engineering")

    df = pd.read_csv(data_path)
    df["sepal_area"] = df["sepal_length"] * df["sepal_width"]
    df["petal_area"] = df["petal_length"] * df["petal_width"]

    DATA_DIR = os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/data")
    output_path = os.path.join(DATA_DIR, "processed")
    os.makedirs(output_path, exist_ok=True)

    feature_path = os.path.join(output_path, "features.csv")
    df.to_csv(feature_path, index = False)
    logging.info(f"Features saved to {feature_path}")
    ti.xcom_push(key = "feature_path", value= feature_path)
    return feature_path

