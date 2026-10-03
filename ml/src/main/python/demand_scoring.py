from datetime import timedelta

import mlflow.spark
from mlflow import MlflowClient
from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from demand_features import build_demand_features
from ml_config import FORECAST_HORIZON_DAYS, MODEL_NAME
from mlflow_support import get_champion_version
from serving_publish import stage_dataframe


FORECAST_STAGING_TABLE = "analytics.demand_forecast_staging"
_MAX_FINITE_DOUBLE = 1.7976931348623157e308


def _validate_prediction_column(predictions: DataFrame) -> None:
    invalid = predictions.filter(
        F.col("prediction").isNull()
        | F.isnan("prediction")
        | (F.abs(F.col("prediction")) > F.lit(_MAX_FINITE_DOUBLE))
    )
    if invalid.limit(1).count() > 0:
        raise RuntimeError("Forecast model produced a non-finite prediction.")


def generate_recursive_forecast(
    model: PipelineModel,
    history: DataFrame,
    horizon_days: int = 7,
) -> DataFrame:
    if horizon_days <= 0:
        raise ValueError("horizon_days must be greater than zero")

    base_history = history.select(
        F.col("productId"),
        F.to_date("eventDate").alias("eventDate"),
        F.col("unitsSold").cast("double").alias("unitsSold"),
    )

    max_date = (
        base_history
        .agg(F.max("eventDate").alias("maxDate"))
        .first()["maxDate"]
    )
    if max_date is None:
        raise ValueError("history must not be empty")

    products = base_history.select("productId").distinct().cache()
    product_count = products.count()
    if product_count == 0:
        raise ValueError("history must contain at least one product")

    working_history = base_history
    forecast = None

    for day_offset in range(1, horizon_days + 1):
        next_date = max_date + timedelta(days=day_offset)
        future_rows = (
            products
            .withColumn(
                "eventDate",
                F.lit(next_date).cast("date"),
            )
            .withColumn(
                "unitsSold",
                F.lit(None).cast("double"),
            )
        )

        scoring_history = working_history.unionByName(future_rows)
        scoring_features = (
            build_demand_features(scoring_history)
            .filter(F.col("eventDate") == F.lit(next_date))
        )

        if scoring_features.count() != product_count:
            raise RuntimeError(
                "Unable to build forecast features for every known product."
            )

        predictions = model.transform(scoring_features)
        _validate_prediction_column(predictions)

        step_forecast = predictions.select(
            "productId",
            F.col("eventDate").alias("forecastDate"),
            F.greatest(
                F.lit(0.0),
                F.col("prediction").cast("double"),
            ).alias("predictedUnits"),
        )

        forecast = (
            step_forecast
            if forecast is None
            else forecast.unionByName(step_forecast)
        )

        working_history = working_history.unionByName(
            step_forecast.select(
                "productId",
                F.col("forecastDate").alias("eventDate"),
                F.col("predictedUnits").alias("unitsSold"),
            )
        )

    products.unpersist()
    if forecast is None:
        raise RuntimeError("Forecast generation produced no rows.")

    expected_rows = product_count * horizon_days
    if forecast.count() != expected_rows:
        raise RuntimeError(
            f"Forecast row count mismatch: expected {expected_rows}."
        )

    return forecast


def score_and_stage_forecast(
    *,
    history: DataFrame,
    client: MlflowClient,
    jdbc_options: dict[str, str],
    stage_fn=stage_dataframe,
    model_loader=mlflow.spark.load_model,
    horizon_days: int = FORECAST_HORIZON_DAYS,
) -> DataFrame:
    champion = get_champion_version(client, MODEL_NAME)
    if champion is None:
        raise RuntimeError(
            "Demand forecast champion alias is not available."
        )

    model = model_loader(
        f"models:/{MODEL_NAME}@champion"
    )
    forecast = generate_recursive_forecast(
        model,
        history,
        horizon_days=horizon_days,
    )

    training_cutoff = (
        history
        .agg(F.max("eventDate").alias("trainingCutoffDate"))
        .first()["trainingCutoffDate"]
    )

    enriched = (
        forecast
        .withColumn("modelName", F.lit(MODEL_NAME))
        .withColumn(
            "modelVersion",
            F.lit(str(champion.version)),
        )
        .withColumn(
            "trainingCutoffDate",
            F.lit(training_cutoff).cast("date"),
        )
        .withColumn("generatedAt", F.current_timestamp())
    )

    staging = enriched.select(
        F.col("productId").alias("product_id"),
        F.col("forecastDate").alias("forecast_date"),
        F.col("predictedUnits").alias("predicted_units"),
        F.col("modelName").alias("model_name"),
        F.col("modelVersion").alias("model_version"),
        F.col("trainingCutoffDate").alias("training_cutoff_date"),
        F.col("generatedAt").alias("generated_at"),
    )

    stage_fn(
        staging,
        FORECAST_STAGING_TABLE,
        jdbc_options,
    )

    return enriched
