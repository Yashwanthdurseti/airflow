import os
import pandas as pd

def ingest_data(**context):
    """
    Simulates data ingestion.
    Later this can be replaced by S3, GCS, DB, APIs, etc.
    """
    print("Starting data ingestion...")


    # For now, use a simple public dataset
    data_url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv"
    
    df = pd.read_csv(data_url)
    DATA_DIR = os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/data")

    output_path = os.path.join(DATA_DIR, "raw")
    os.makedirs(output_path, exist_ok=True)

    file_path = os.path.join(output_path, "iris.csv")
    df.to_csv(file_path, index=False)

    print(f"Data saved to {file_path}")
    return file_path
