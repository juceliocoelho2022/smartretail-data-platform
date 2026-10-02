from pyspark.sql import SparkSession

from config import (
    BRONZE_CHECKPOINT_LOCATION,
    BRONZE_PATH,
    CHECKPOINT_LOCATION,
    KAFKA_BOOTSTRAP_SERVERS,
    ORDERS_TOPIC,
    S3_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_KEY,
    SPARK_APP_NAME,
)

from transforms import (
    build_bronze_events,
    build_sales_metrics,
    parse_order_events,
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(SPARK_APP_NAME)

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

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )
        .option(
            "subscribe",
            ORDERS_TOPIC
        )
        .option(
            "startingOffsets",
            "earliest"
        )
        .load()
    )

    # ----------------------------------------
    # Bronze Lakehouse
    # ----------------------------------------

    bronze_events = build_bronze_events(
        kafka_stream
    )

    bronze_query = (
        bronze_events
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option(
            "path",
            BRONZE_PATH
        )
        .option(
            "checkpointLocation",
            BRONZE_CHECKPOINT_LOCATION
        )
        .partitionBy(
            "ingestionDate"
        )
        .queryName(
            "smartretail-bronze-orders"
        )
        .start()
    )

    # ----------------------------------------
    # Real-time metrics
    # ----------------------------------------

    order_events = parse_order_events(
        kafka_stream
    )

    sales_metrics = build_sales_metrics(
        order_events
    )

    metrics_query = (
        sales_metrics
        .writeStream
        .outputMode("update")
        .format("console")
        .option(
            "truncate",
            False
        )
        .option(
            "checkpointLocation",
            CHECKPOINT_LOCATION
        )
        .queryName(
            "smartretail-sales-metrics"
        )
        .start()
    )

    print(
        f"Bronze query started: "
        f"{bronze_query.name}"
    )

    print(
        f"Metrics query started: "
        f"{metrics_query.name}"
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()