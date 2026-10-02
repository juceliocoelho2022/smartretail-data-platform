import mlflow
import mlflow.spark
from mlflow import MlflowClient

from ml_config import MLFLOW_TRACKING_URI, MODEL_NAME
from mlflow_support import configure_mlflow, get_champion_version


EXPERIMENT_NAME = "smartretail-demand-forecast"


def main() -> None:
    configure_mlflow(
        MLFLOW_TRACKING_URI,
        EXPERIMENT_NAME,
    )
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    client = MlflowClient()
    champion = get_champion_version(
        client,
        MODEL_NAME,
    )
    if champion is None:
        raise RuntimeError(
            "Demand forecast champion alias is not available."
        )

    mlflow.spark.load_model(
        f"models:/{MODEL_NAME}@champion"
    )

    print("Demand champion validation: PASSED")
    print(f"Model={MODEL_NAME}")
    print(f"Champion version={champion.version}")


if __name__ == "__main__":
    main()
