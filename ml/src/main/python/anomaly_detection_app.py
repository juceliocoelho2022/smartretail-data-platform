from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StructField, StructType

from anomaly_detection import (
    calculate_mad,
    calculate_median,
    calculate_modified_z_score,
    is_anomaly,
)


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


def score_anomaly_candidates(
    candidates: DataFrame,
    threshold: float = 3.5,
) -> DataFrame:
    score_schema = StructType(
        [
            StructField("anomaly_score", DoubleType(), nullable=False),
            StructField("is_anomaly", BooleanType(), nullable=False),
        ]
    )

    def score_row(
        residual: float,
        historical_residuals: list[float],
    ) -> tuple[float, bool]:
        median_residual = calculate_median(historical_residuals)
        mad = calculate_mad(historical_residuals)
        score = calculate_modified_z_score(
            residual=float(residual),
            median_residual=median_residual,
            mad=mad,
        )
        return float(score), is_anomaly(score, threshold=threshold)

    score_udf = F.udf(score_row, score_schema)

    scored = (
        candidates
        .filter(F.size(F.col("historical_residuals")) > 0)
        .withColumn(
            "anomaly",
            score_udf(
                F.col("residual"),
                F.col("historical_residuals"),
            ),
        )
        .withColumn(
            "anomaly_score",
            F.col("anomaly.anomaly_score"),
        )
        .withColumn(
            "is_anomaly",
            F.col("anomaly.is_anomaly"),
        )
        .withColumn(
            "detected_at",
            F.current_timestamp(),
        )
    )

    return scored.select(
        F.to_date("event_date").alias("event_date"),
        "product_id",
        F.col("actual_units").cast("double").alias("actual_units"),
        F.col("expected_units").cast("double").alias("expected_units"),
        F.col("residual").cast("double").alias("residual"),
        F.col("anomaly_score").cast("double").alias("anomaly_score"),
        F.col("is_anomaly").cast("boolean").alias("is_anomaly"),
        F.col("model_version").cast("string").alias("model_version"),
        "detected_at",
    )
