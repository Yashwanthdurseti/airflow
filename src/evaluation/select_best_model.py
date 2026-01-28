import mlflow

def select_best_model(**context):
    ti = context["ti"]

    models = ["logistic", "random_forest", "xgboost", "lightgbm"]
    accuracies = {}

    for model in models:
        acc = ti.xcom_pull(
            task_ids=f"train_{model}",
            key=f"{model}_accuracy"
        )
        if acc is not None:
            accuracies[model] = acc

    if not accuracies:
        raise ValueError("No model accuracies found")

    best_model = max(accuracies, key=accuracies.get)
    best_score = accuracies[best_model]

    print(f"Best model: {best_model} | Accuracy: {best_score}")

    ti.xcom_push(key="best_model", value=best_model)
    ti.xcom_push(key="best_score", value=best_score)

    return best_model
