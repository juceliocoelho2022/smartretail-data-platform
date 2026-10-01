from pyspark.sql import SparkSession

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    ORDERS_TOPIC,
    SPARK_APP_NAME,
    CHECKPOINT_LOCATION,
)

from transforms import (
    parse_order_events,
    build_sales_metrics,
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

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

    order_events = parse_order_events(
        kafka_stream
    )

    sales_metrics = build_sales_metrics(
        order_events
    )

    query = (
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
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()