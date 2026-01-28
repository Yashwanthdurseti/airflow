import mlflow
from mlflow.tracking import MlflowClient

def register_best_model(**context):
    ti = context["ti"]

    best_model = ti.xcom_pull(
        task_ids="select_best_model",
        key="best_model"
    )

    if not best_model:
        raise ValueError("Best model not found in XCom")

    client = MlflowClient()

    experiment = client.get_experiment_by_name("Default")
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=f"params.model = '{best_model}'",
        order_by=["metrics.accuracy DESC"],
        max_results=1
    )

    if not runs:
        raise ValueError("No MLflow run found for best model")

    best_run = runs[0]
    model_uri = f"runs:/{best_run.info.run_id}/model"

    registered_model_name = "iris_classifier"

    result = mlflow.register_model(
        model_uri=model_uri,
        name=registered_model_name
    )

    print(
        f"Registered model: {registered_model_name} | "
        f"Version: {result.version}"
    )

    ti.xcom_push(key="model_name", value=registered_model_name)
    ti.xcom_push(key="model_version", value=result.version)

    return result.version
