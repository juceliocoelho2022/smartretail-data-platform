from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
)


def build_summary_read_model(
    gold_summary: DataFrame,
) -> DataFrame:
    return gold_summary.select(
        lit(1).cast("short").alias("id"),
        col("totalOrders").alias(
            "total_orders"
        ),
        col("totalItems").alias(
            "total_items"
        ),
        col("totalRevenue").alias(
            "total_revenue"
        ),
        col("averageOrderValue").alias(
            "average_order_value"
        ),
        col("uniqueCustomers").alias(
            "unique_customers"
        ),
        col("uniqueProducts").alias(
            "unique_products"
        ),
        col("goldProcessedAt").alias(
            "gold_processed_at"
        ),
        current_timestamp().alias(
            "refreshed_at"
        ),
    )


def build_daily_read_model(
    gold_daily: DataFrame,
) -> DataFrame:
    return gold_daily.select(
        col("eventDate").alias(
            "event_date"
        ),
        col("channel"),
        col("location"),
        col("totalOrders").alias(
            "total_orders"
        ),
        col("totalItems").alias(
            "total_items"
        ),
        col("totalRevenue").alias(
            "total_revenue"
        ),
        col("averageOrderValue").alias(
            "average_order_value"
        ),
        col("goldProcessedAt").alias(
            "gold_processed_at"
        ),
        current_timestamp().alias(
            "refreshed_at"
        ),
    )
