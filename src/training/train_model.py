import os
import pandas as pd
import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier

try:
    from xgboost import XGBClassifier
except ImportError:
    XGBClassifier = None

try:
    from lightgbm import LGBMClassifier
except ImportError:
    LGBMClassifier = None


MODEL_REGISTRY = {
    "logistic": LogisticRegression(max_iter=200),
    "random_forest": RandomForestClassifier(n_estimators=100),
    "xgboost": XGBClassifier(use_label_encoder=False, eval_metric="mlogloss") if XGBClassifier else None,
    "lightgbm": LGBMClassifier() if LGBMClassifier else None,
}


def train_model(model_name: str, **context):
    ti = context["ti"]
    feature_path = ti.xcom_pull(task_ids="build_features", key="feature_path")

    if model_name not in MODEL_REGISTRY or MODEL_REGISTRY[model_name] is None:
        raise ValueError(f"Model {model_name} not available")

    df = pd.read_csv(feature_path)

    X = df.drop("species", axis=1)
    y = df["species"]

    if model_name == "xgboost":
        from sklearn.preprocessing import LabelEncoder
        le = LabelEncoder()
        y = le.fit_transform(y)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = MODEL_REGISTRY[model_name]

    with mlflow.start_run(run_name=model_name):
        model.fit(X_train, y_train)
        preds = model.predict(X_test)

        acc = accuracy_score(y_test, preds)

        mlflow.log_param("model", model_name)
        mlflow.log_metric("accuracy", acc)
        mlflow.sklearn.log_model(model, artifact_path="model")

    print(f"{model_name} accuracy: {acc}")

    ti.xcom_push(key=f"{model_name}_accuracy", value=acc)
    return acc
