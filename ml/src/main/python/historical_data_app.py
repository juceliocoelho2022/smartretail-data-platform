from datetime import date

from pyspark.sql import SparkSession

from historical_data_generator import generate_silver_history
from ml_config import (
    ML_HISTORY_DAYS,
    ML_HISTORY_SILVER_PATH,
    ML_HISTORY_START_DATE,
    ML_PRODUCT_COUNT,
    ML_RANDOM_SEED,
    S3_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_KEY,
)


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("smartretail-ml-history-generator")
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
        history = generate_silver_history(
            spark,
            date.fromisoformat(ML_HISTORY_START_DATE),
            ML_HISTORY_DAYS,
            ML_PRODUCT_COUNT,
            ML_RANDOM_SEED,
        )

        (
            history
            .write
            .mode("overwrite")
            .partitionBy("eventDate")
            .parquet(ML_HISTORY_SILVER_PATH)
        )

        print("ML historical data generation: PASSED")
        print(f"Rows={history.count()}")
        print(f"Products={history.select('productId').distinct().count()}")
        print(f"Dates={history.select('eventDate').distinct().count()}")
        print(f"Output={ML_HISTORY_SILVER_PATH}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
