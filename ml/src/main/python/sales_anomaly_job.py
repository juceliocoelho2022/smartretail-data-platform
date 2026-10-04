from pyspark.sql import DataFrame, SparkSession

from anomaly_detection_app import (
    attach_residual_history,
    build_anomaly_candidates,
    score_anomaly_candidates,
    select_temporally_valid_forecasts,
    stage_anomaly_results,
)
from ml_config import (
    ANALYTICS_JDBC_PASSWORD,
    ANALYTICS_JDBC_URL,
    ANALYTICS_JDBC_USER,
    ANALYTICS_PG_DSN,
    ANOMALY_STAGING_TABLE,
    ANOMALY_TARGET_TABLE,
    FORECAST_TARGET_TABLE,
    PRODUCT_DEMAND_GOLD_PATH,
    S3_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_KEY,
)
from publish_anomaly_app import ANOMALY_COLUMNS, ANOMALY_CONFLICT_COLUMNS
from serving_publish import merge_from_staging


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("smartretail-sales-anomaly")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint.region", S3_REGION)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def _spark_jdbc_options(jdbc_options: dict[str, str]) -> dict[str, str]:
    options = {
        key: value
        for key, value in jdbc_options.items()
        if key != "pg_dsn"
    }
    return options


def load_actuals(spark: SparkSession) -> DataFrame:
    return spark.read.parquet(PRODUCT_DEMAND_GOLD_PATH)


def load_forecasts(
    spark: SparkSession,
    jdbc_options: dict[str, str],
) -> DataFrame:
    options = _spark_jdbc_options(jdbc_options)
    options["dbtable"] = FORECAST_TARGET_TABLE
    return (
        spark.read
        .format("jdbc")
        .options(**options)
        .load()
    )


def run_sales_anomaly_job(
    spark: SparkSession,
    jdbc_options: dict[str, str],
) -> dict[str, int]:
    actual = load_actuals(spark)
    forecast = load_forecasts(spark, jdbc_options)
    selected_forecast = select_temporally_valid_forecasts(forecast)

    candidates = build_anomaly_candidates(
        actual=actual,
        forecast=selected_forecast,
    )
    candidate_count = candidates.count()

    with_history = attach_residual_history(candidates)
    scored = score_anomaly_candidates(with_history)
    scored_count = scored.count()
    anomaly_count = scored.filter("is_anomaly = true").count()

    stage_anomaly_results(
        scored,
        jdbc_options=jdbc_options,
    )
    merge_from_staging(
        target_table=ANOMALY_TARGET_TABLE,
        staging_table=ANOMALY_STAGING_TABLE,
        ordered_columns=ANOMALY_COLUMNS,
        conflict_columns=ANOMALY_CONFLICT_COLUMNS,
        pg_dsn=jdbc_options["pg_dsn"],
    )

    return {
        "candidate_count": candidate_count,
        "scored_count": scored_count,
        "anomaly_count": anomaly_count,
    }


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    jdbc_options = {
        "url": ANALYTICS_JDBC_URL,
        "user": ANALYTICS_JDBC_USER,
        "password": ANALYTICS_JDBC_PASSWORD,
        "driver": "org.postgresql.Driver",
        "pg_dsn": ANALYTICS_PG_DSN,
    }

    try:
        metrics = run_sales_anomaly_job(spark, jdbc_options)
        print("Sales anomaly job: PASSED")
        print(f"Candidates={metrics['candidate_count']}")
        print(f"Scored={metrics['scored_count']}")
        print(f"Anomalies={metrics['anomaly_count']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
