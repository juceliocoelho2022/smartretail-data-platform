from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    from_json,
    round as spark_round,
    sum as spark_sum,
    timestamp_seconds,
    to_date,
    window,
)

from schemas import ORDER_EVENT_SCHEMA


def build_bronze_events(
        kafka_stream: DataFrame
) -> DataFrame:
    """
    Preserve the original Kafka event and metadata
    for auditability and reprocessing.
    """
    return (
        kafka_stream
        .selectExpr(
            "CAST(key AS STRING) AS kafkaKey",
            "CAST(value AS STRING) AS payload",
            "topic AS kafkaTopic",
            "partition AS kafkaPartition",
            "offset AS kafkaOffset",
            "timestamp AS kafkaTimestamp"
        )
        .withColumn(
            "ingestedAt",
            current_timestamp()
        )
        .withColumn(
            "ingestionDate",
            to_date(col("ingestedAt"))
        )
    )


def parse_order_events(
        kafka_stream: DataFrame
) -> DataFrame:
    return (
        kafka_stream
        .selectExpr(
            "CAST(value AS STRING) AS json"
        )
        .select(
            from_json(
                col("json"),
                ORDER_EVENT_SCHEMA
            ).alias("event")
        )
        .select("event.*")
        .withColumn(
            "occurredAt",
            timestamp_seconds(
                col("occurredAt")
            )
        )
        .filter(
            col("eventId").isNotNull()
            & col("occurredAt").isNotNull()
        )
    )


def build_sales_metrics(
        order_events: DataFrame
) -> DataFrame:
    return (
        order_events
        .withColumn(
            "revenue",
            col("quantity")
            * col("unitPrice")
        )
        .withWatermark(
            "occurredAt",
            "2 minutes"
        )
        .groupBy(
            window(
                col("occurredAt"),
                "1 minute"
            ),
            col("channel")
        )
        .agg(
            count("*").alias("orders"),
            spark_sum(
                "quantity"
            ).alias("items"),
            spark_round(
                spark_sum("revenue"),
                2
            ).alias("revenue")
        )
    )