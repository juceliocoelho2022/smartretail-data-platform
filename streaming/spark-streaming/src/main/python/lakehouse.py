from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    current_timestamp,
    from_json,
    round as spark_round,
    sum as spark_sum,
    timestamp_seconds,
    to_date,
    trim,
    upper,
)
from pyspark.sql.types import DecimalType

from schemas import ORDER_EVENT_SCHEMA


def build_silver_orders(
        bronze_events: DataFrame
) -> DataFrame:

    parsed = (
        bronze_events
        .select(
            from_json(
                col("payload"),
                ORDER_EVENT_SCHEMA
            ).alias("event"),

            col("kafkaTopic"),
            col("kafkaPartition"),
            col("kafkaOffset"),
            col("kafkaTimestamp"),

            col("ingestedAt").alias(
                "bronzeIngestedAt"
            ),
        )
        .select(
            "event.*",
            "kafkaTopic",
            "kafkaPartition",
            "kafkaOffset",
            "kafkaTimestamp",
            "bronzeIngestedAt",
        )
    )

    normalized = (
        parsed
        .withColumn(
            "occurredAt",
            timestamp_seconds(
                col("occurredAt")
            )
        )
        .withColumn(
            "channel",
            upper(
                trim(col("channel"))
            )
        )
        .withColumn(
            "location",
            upper(
                trim(col("location"))
            )
        )
        .withColumn(
            "revenue",
            (
                    col("quantity")
                    * col("unitPrice")
            ).cast(
                DecimalType(19, 2)
            )
        )
        .withColumn(
            "eventDate",
            to_date(
                col("occurredAt")
            )
        )
        .withColumn(
            "silverProcessedAt",
            current_timestamp()
        )
    )

    validated = (
        normalized
        .filter(
            col("eventId").isNotNull()
            & col("occurredAt").isNotNull()
            & col("customerId").isNotNull()
            & col("productId").isNotNull()
            & (col("quantity") > 0)
            & (col("unitPrice") >= 0)
        )
    )

    return (
        validated
        .withWatermark(
            "occurredAt",
            "10 minutes"
        )
        .dropDuplicates([
            "eventId"
        ])
    )
def build_gold_daily_sales(
        silver_orders: DataFrame
) -> DataFrame:
    """
    Builds analytical KPIs grouped by
    event date, sales channel and location.
    """

    return (
        silver_orders
        .groupBy(
            "eventDate",
            "channel",
            "location",
        )
        .agg(
            countDistinct(
                "eventId"
            ).alias(
                "totalOrders"
            ),

            spark_sum(
                "quantity"
            ).alias(
                "totalItems"
            ),

            spark_round(
                spark_sum("revenue"),
                2,
            )
            .cast(
                DecimalType(19, 2)
            )
            .alias(
                "totalRevenue"
            ),

            spark_round(
                avg("revenue"),
                2,
            )
            .cast(
                DecimalType(19, 2)
            )
            .alias(
                "averageOrderValue"
            ),
            )
        .withColumn(
            "goldProcessedAt",
            current_timestamp()
        )
    )


def build_gold_summary(
        silver_orders: DataFrame
) -> DataFrame:
    """
    Builds the global analytical snapshot
    of the orders dataset.
    """

    return (
        silver_orders
        .agg(
            countDistinct(
                "eventId"
            ).alias(
                "totalOrders"
            ),

            spark_sum(
                "quantity"
            ).alias(
                "totalItems"
            ),

            spark_round(
                spark_sum("revenue"),
                2,
            )
            .cast(
                DecimalType(19, 2)
            )
            .alias(
                "totalRevenue"
            ),

            spark_round(
                avg("revenue"),
                2,
            )
            .cast(
                DecimalType(19, 2)
            )
            .alias(
                "averageOrderValue"
            ),

            countDistinct(
                "customerId"
            ).alias(
                "uniqueCustomers"
            ),

            countDistinct(
                "productId"
            ).alias(
                "uniqueProducts"
            ),
            )
        .withColumn(
            "goldProcessedAt",
            current_timestamp()
        )
    )