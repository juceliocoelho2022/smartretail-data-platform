import math

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def evaluate_regression(
    predictions: DataFrame,
    label_col: str = "unitsSold",
    prediction_col: str = "prediction",
) -> dict[str, float]:
    metrics_row = (
        predictions
        .select(
            F.col(label_col).cast("double").alias("label"),
            F.col(prediction_col).cast("double").alias("prediction"),
        )
        .select(
            F.abs(F.col("label") - F.col("prediction")).alias("absoluteError"),
            F.pow(F.col("label") - F.col("prediction"), 2).alias("squaredError"),
            F.abs(F.col("label")).alias("absoluteLabel"),
        )
        .agg(
            F.avg("absoluteError").alias("mae"),
            F.sqrt(F.avg("squaredError")).alias("rmse"),
            F.sum("absoluteError").alias("absoluteErrorSum"),
            F.sum("absoluteLabel").alias("absoluteLabelSum"),
            F.count(F.lit(1)).alias("rowCount"),
        )
        .first()
    )

    if metrics_row["rowCount"] == 0:
        raise ValueError("predictions must not be empty")

    absolute_error_sum = float(metrics_row["absoluteErrorSum"])
    absolute_label_sum = float(metrics_row["absoluteLabelSum"])

    if absolute_label_sum == 0.0:
        wape = 0.0 if absolute_error_sum == 0.0 else math.inf
    else:
        wape = absolute_error_sum / absolute_label_sum

    return {
        "mae": float(metrics_row["mae"]),
        "rmse": float(metrics_row["rmse"]),
        "wape": float(wape),
    }


def build_seasonal_naive_predictions(
    features: DataFrame,
) -> DataFrame:
    return features.withColumn(
        "prediction",
        F.col("lag7").cast("double"),
    )
