from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_anomaly_candidates(
    actual: DataFrame,
    forecast: DataFrame,
) -> DataFrame:
    actual_normalized = actual.select(
        F.col("productId").alias("product_id"),
        F.to_date("eventDate").alias("event_date"),
        F.col("unitsSold").cast("double").alias("actual_units"),
    )

    forecast_normalized = forecast.select(
        F.col("product_id"),
        F.to_date("forecast_date").alias("event_date"),
        F.col("predicted_units").cast("double").alias("expected_units"),
        F.col("model_version"),
    )

    return (
        actual_normalized
        .join(
            forecast_normalized,
            on=["product_id", "event_date"],
            how="inner",
        )
        .withColumn(
            "residual",
            F.col("actual_units") - F.col("expected_units"),
        )
        .select(
            "event_date",
            "product_id",
            "actual_units",
            "expected_units",
            "residual",
            "model_version",
        )
    )


def attach_residual_history(candidates: DataFrame) -> DataFrame:
    history_window = (
        Window
        .partitionBy("product_id")
        .orderBy(F.col("event_date"))
        .rowsBetween(Window.unboundedPreceding, -1)
    )

    return candidates.withColumn(
        "historical_residuals",
        F.collect_list(F.col("residual")).over(history_window),
    )
