from pyspark.sql import SparkSession

from config import (
    GOLD_DAILY_PATH,
    GOLD_SUMMARY_PATH,
    S3_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_KEY,
    SILVER_PATH,
)

from lakehouse import (
    build_gold_daily_sales,
    build_gold_summary,
)


GOLD_APP_NAME = (
    "smartretail-gold-orders"
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(
            GOLD_APP_NAME
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
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
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
            "Gold cannot be generated."
        )

    print(
        f"Silver records: "
        f"{silver_orders.count()}"
    )

    gold_daily = (
        build_gold_daily_sales(
            silver_orders
        )
    )

    gold_summary = (
        build_gold_summary(
            silver_orders
        )
    )

    print(
        "Writing Gold daily sales..."
    )

    (
        gold_daily
        .write
        .mode("overwrite")
        .format("parquet")
        .partitionBy(
            "eventDate"
        )
        .save(
            GOLD_DAILY_PATH
        )
    )

    print(
        "Writing Gold summary..."
    )

    (
        gold_summary
        .coalesce(1)
        .write
        .mode("overwrite")
        .format("parquet")
        .save(
            GOLD_SUMMARY_PATH
        )
    )

    print(
        "Gold layer successfully generated."
    )

    print(
        f"Daily dataset: "
        f"{GOLD_DAILY_PATH}"
    )

    print(
        f"Summary dataset: "
        f"{GOLD_SUMMARY_PATH}"
    )

    print(
        "Gold summary:"
    )

    gold_summary.show(
        truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    main()