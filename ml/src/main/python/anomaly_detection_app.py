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


def _median_from_array(values):
    sorted_values = F.array_sort(values)
    count = F.size(sorted_values)
    upper_index = F.floor(count / F.lit(2)).cast("int")
    lower_index = upper_index - F.lit(1)

    odd_median = F.get(sorted_values, upper_index)
    even_median = (
        F.get(sorted_values, lower_index)
        + F.get(sorted_values, upper_index)
    ) / F.lit(2.0)

    return F.when(
        F.pmod(count, F.lit(2)) == F.lit(1),
        odd_median,
    ).otherwise(even_median)


def score_anomaly_candidates(
    candidates: DataFrame,
    threshold: float = 3.5,
) -> DataFrame:
    eligible = candidates.filter(
        F.size(F.col("historical_residuals")) > 0
    )

    median_residual = _median_from_array(
        F.col("historical_residuals")
    )

    absolute_deviations = F.transform(
        F.col("historical_residuals"),
        lambda value: F.abs(value - median_residual),
    )
    mad = _median_from_array(absolute_deviations)

    difference = F.col("residual") - median_residual
    finite_score = (
        F.lit(0.6745)
        * difference
        / mad
    )

    anomaly_score = (
        F.when(
            (mad == F.lit(0.0))
            & (difference == F.lit(0.0)),
            F.lit(0.0),
        )
        .when(
            (mad == F.lit(0.0))
            & (difference > F.lit(0.0)),
            F.lit(float("inf")),
        )
        .when(
            mad == F.lit(0.0),
            F.lit(float("-inf")),
        )
        .otherwise(finite_score)
    )

    scored = (
        eligible
        .withColumn("anomaly_score", anomaly_score.cast("double"))
        .withColumn(
            "is_anomaly",
            (F.abs(F.col("anomaly_score")) > F.lit(threshold)).cast("boolean"),
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
