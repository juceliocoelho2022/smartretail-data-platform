import mlflow
import mlflow.spark
from mlflow import MlflowClient
from pyspark.sql import SparkSession

from ml_config import MLFLOW_TRACKING_URI, MODEL_NAME
from mlflow_support import configure_mlflow, get_champion_version


EXPERIMENT_NAME = "smartretail-demand-forecast"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("smartretail-demand-champion-validation")
        .getOrCreate()
    )


def validate_champion() -> None:
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


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        validate_champion()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
