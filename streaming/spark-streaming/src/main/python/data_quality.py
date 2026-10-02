from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    lit,
    sum as spark_sum,
    when,
)


QUALITY_RULES = (
    "nullEventId",
    "nullCustomerId",
    "nullProductId",
    "invalidQuantity",
    "invalidUnitPrice",
    "duplicateRows",
)


def build_silver_quality_metrics(
    silver_orders: DataFrame,
) -> DataFrame:
    base_metrics = (
        silver_orders
        .agg(
            count("*").alias(
                "totalRows"
            ),
            spark_sum(
                when(
                    col("eventId").isNull(),
                    1,
                ).otherwise(0)
            ).cast("long").alias(
                "nullEventId"
            ),
            spark_sum(
                when(
                    col("customerId").isNull(),
                    1,
                ).otherwise(0)
            ).cast("long").alias(
                "nullCustomerId"
            ),
            spark_sum(
                when(
                    col("productId").isNull(),
                    1,
                ).otherwise(0)
            ).cast("long").alias(
                "nullProductId"
            ),
            spark_sum(
                when(
                    col("quantity") <= 0,
                    1,
                ).otherwise(0)
            ).cast("long").alias(
                "invalidQuantity"
            ),
            spark_sum(
                when(
                    col("unitPrice") < 0,
                    1,
                ).otherwise(0)
            ).cast("long").alias(
                "invalidUnitPrice"
            ),
        )
    )

    duplicate_metrics = (
        silver_orders
        .filter(
            col("eventId").isNotNull()
        )
        .groupBy(
            "eventId"
        )
        .count()
        .filter(
            col("count") > 1
        )
        .agg(
            coalesce(
                spark_sum(
                    col("count") - lit(1)
                ),
                lit(0),
            ).cast("long").alias(
                "duplicateRows"
            )
        )
    )

    return base_metrics.crossJoin(
        duplicate_metrics
    )


def collect_silver_quality_metrics(
    silver_orders: DataFrame,
) -> dict:
    row = (
        build_silver_quality_metrics(
            silver_orders
        )
        .first()
    )

    if row is None:
        return {
            "totalRows": 0,
            **{
                rule: 0
                for rule in QUALITY_RULES
            },
        }

    return row.asDict()


def find_quality_failures(
    metrics: dict,
) -> dict:
    return {
        rule: int(
            metrics.get(
                rule,
                0,
            )
            or 0
        )
        for rule in QUALITY_RULES
        if int(
            metrics.get(
                rule,
                0,
            )
            or 0
        ) > 0
    }


def assert_silver_quality(
    silver_orders: DataFrame,
) -> dict:
    metrics = collect_silver_quality_metrics(
        silver_orders
    )

    failures = find_quality_failures(
        metrics
    )

    if failures:
        raise RuntimeError(
            "Silver data quality gate failed: "
            f"{failures}"
        )

    return metrics
