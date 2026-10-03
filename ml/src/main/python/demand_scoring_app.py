import mlflow
from mlflow import MlflowClient
from pyspark.sql import SparkSession

from demand_scoring import score_and_stage_forecast
from ml_config import (
    ANALYTICS_JDBC_PASSWORD,
    ANALYTICS_JDBC_URL,
    ANALYTICS_JDBC_USER,
    ANALYTICS_PG_DSN,
    MLFLOW_TRACKING_URI,
    PRODUCT_DEMAND_GOLD_PATH,
    S3_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_KEY,
)


EXPERIMENT_NAME = "smartretail-demand-forecast"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("smartretail-demand-scoring")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint.region", S3_REGION)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment(EXPERIMENT_NAME)

        history = spark.read.parquet(PRODUCT_DEMAND_GOLD_PATH)
        jdbc_options = {
            "url": ANALYTICS_JDBC_URL,
            "user": ANALYTICS_JDBC_USER,
            "password": ANALYTICS_JDBC_PASSWORD,
            "driver": "org.postgresql.Driver",
            "pg_dsn": ANALYTICS_PG_DSN,
        }

        forecast = score_and_stage_forecast(
            history=history,
            client=MlflowClient(),
            jdbc_options=jdbc_options,
        )

        print("Demand forecast staging: PASSED")
        print(f"Rows={forecast.count()}")
        print(
            "Products="
            f"{forecast.select('productId').distinct().count()}"
        )
        print(
            "Forecast dates="
            f"{forecast.select('forecastDate').distinct().count()}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
