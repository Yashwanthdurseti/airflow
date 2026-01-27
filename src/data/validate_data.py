import os
import pandas as pd

EXPECTED_COLUMNS = {
    "sepal_length",
    "sepal_width",
    "petal_length",
    "petal_width",
    "species",
}

def validate_data(**context):
    ti = context["ti"]

    # Pull file path from previous task
    data_path = ti.xcom_pull(task_ids="ingest_data")

    if not data_path or not os.path.exists(data_path):
        raise ValueError("Ingested data file not found")

    df = pd.read_csv(data_path)

    print(f"Validating data at {data_path}")

    # Basic validations
    if df.empty:
        raise ValueError("Dataset is empty")

    if set(df.columns) != EXPECTED_COLUMNS:
        raise ValueError(f"Schema mismatch: {df.columns}")

    row_count = len(df)

    print(f"Validation successful | Rows: {row_count}")

    # Push metadata forward
    ti.xcom_push(key="row_count", value=row_count)

    return "validation_passed"
