from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ml_config import ANOMALY_STAGING_TABLE
from serving_publish import stage_dataframe


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


def _median_array(column_name: str):
    values = F.array_sort(F.col(column_name))
    count = F.size(values)
    lower_index = F.floor((count - F.lit(1)) / F.lit(2)).cast("int")
    upper_index = F.floor(count / F.lit(2)).cast("int")

    return (
        F.element_at(values, lower_index + F.lit(1))
        + F.element_at(values, upper_index + F.lit(1))
    ) / F.lit(2.0)


def score_anomaly_candidates(
    candidates: DataFrame,
    threshold: float = 3.5,
) -> DataFrame:
    with_history = candidates.filter(
        F.size(F.col("historical_residuals")) > 0
    )

    with_median = with_history.withColumn(
        "median_residual",
        _median_array("historical_residuals"),
    )

    with_deviations = with_median.withColumn(
        "absolute_deviations",
        F.transform(
            F.col("historical_residuals"),
            lambda value: F.abs(value - F.col("median_residual")),
        ),
    )

    with_mad = with_deviations.withColumn(
        "mad",
        _median_array("absolute_deviations"),
    )

    difference = F.col("residual") - F.col("median_residual")

    with_score = with_mad.withColumn(
        "anomaly_score",
        F.when(
            F.col("mad") == 0.0,
            F.when(
                difference == 0.0,
                F.lit(0.0),
            ).otherwise(
                F.when(difference > 0.0, F.lit(float("inf")))
                .otherwise(F.lit(float("-inf")))
            ),
        ).otherwise(
            F.lit(0.6745) * difference / F.col("mad")
        ),
    )

    scored = (
        with_score
        .withColumn(
            "is_anomaly",
            F.abs(F.col("anomaly_score")) > F.lit(threshold),
        )
        .withColumn("detected_at", F.current_timestamp())
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


def stage_anomaly_results(
    scored: DataFrame,
    jdbc_options: dict[str, str],
    stage_fn=stage_dataframe,
) -> None:
    stage_fn(
        scored,
        ANOMALY_STAGING_TABLE,
        jdbc_options,
    )
