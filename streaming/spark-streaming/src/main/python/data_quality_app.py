from pyspark.sql import SparkSession

from config import (
    S3_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_KEY,
    SILVER_PATH,
)
from data_quality import (
    assert_silver_quality,
)


DATA_QUALITY_APP_NAME = (
    "smartretail-silver-data-quality"
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(
            DATA_QUALITY_APP_NAME
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC"
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            S3_ENDPOINT
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            S3_ACCESS_KEY
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            S3_SECRET_KEY
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false"
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a."
            "SimpleAWSCredentialsProvider"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            S3_REGION
        )
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print(
        f"Reading Silver dataset: "
        f"{SILVER_PATH}"
    )

    silver_orders = (
        spark.read
        .format("parquet")
        .load(
            SILVER_PATH
        )
    )

    if silver_orders.isEmpty():
        raise RuntimeError(
            "Silver dataset is empty. "
            "Data Quality cannot be evaluated."
        )

    metrics = assert_silver_quality(
        silver_orders
    )

    print(
        "Silver Data Quality gate: PASSED"
    )

    for name, value in metrics.items():
        print(
            f"{name}={value}"
        )

    spark.stop()


if __name__ == "__main__":
    main()
