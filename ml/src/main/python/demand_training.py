from dataclasses import dataclass
import math
from typing import Callable

import mlflow
import mlflow.spark
from mlflow import MlflowClient
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.sql import DataFrame

from demand_metrics import evaluate_regression
from mlflow_support import get_champion_version


FEATURE_COLUMNS = (
    "lag1",
    "lag7",
    "lag14",
    "rollingMean7",
    "rollingMean28",
    "rollingStd7",
    "dayOfWeek",
    "dayOfMonth",
    "month",
    "isWeekend",
    "timeIndex",
)


@dataclass(frozen=True)
class CandidateResult:
    name: str
    validation_metrics: dict[str, float]
    run_id: str


def _metrics_are_finite(metrics: dict[str, float]) -> bool:
    required = ("mae", "rmse", "wape")
    return all(
        key in metrics
        and math.isfinite(float(metrics[key]))
        for key in required
    )


def select_eligible_champion(
    results: list[CandidateResult],
    baseline_mae: float,
) -> CandidateResult | None:
    if not math.isfinite(float(baseline_mae)):
        return None

    eligible = [
        result
        for result in results
        if _metrics_are_finite(result.validation_metrics)
        and float(result.validation_metrics["mae"])
        < float(baseline_mae)
    ]

    if not eligible:
        return None

    return min(
        eligible,
        key=lambda result: float(
            result.validation_metrics["mae"]
        ),
    )


def build_candidate_pipelines() -> dict[str, Pipeline]:
    assembler = VectorAssembler(
        inputCols=list(FEATURE_COLUMNS),
        outputCol="features",
        handleInvalid="error",
    )

    random_forest = RandomForestRegressor(
        featuresCol="features",
        labelCol="unitsSold",
        predictionCol="prediction",
        numTrees=40,
        maxDepth=8,
        seed=20261002,
    )

    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="unitsSold",
        predictionCol="prediction",
        maxIter=40,
        maxDepth=5,
        seed=20261002,
    )

    return {
        "random_forest": Pipeline(
            stages=[assembler, random_forest]
        ),
        "gbt": Pipeline(
            stages=[assembler, gbt]
        ),
    }


def train_candidate_runs(
    train_df: DataFrame,
    validation_df: DataFrame,
    baseline_metrics: dict[str, float],
) -> list[CandidateResult]:
    results: list[CandidateResult] = []

    for name, pipeline in build_candidate_pipelines().items():
        with mlflow.start_run(run_name=name) as run:
            model: PipelineModel = pipeline.fit(train_df)
            predictions = model.transform(validation_df)
            metrics = evaluate_regression(predictions)

            mlflow.log_params({
                "model_type": name,
                "feature_count": len(FEATURE_COLUMNS),
                "seed": 20261002,
            })
            mlflow.log_metrics({
                "baseline_validation_mae": float(
                    baseline_metrics["mae"]
                ),
                "validation_mae": metrics["mae"],
                "validation_rmse": metrics["rmse"],
                "validation_wape": metrics["wape"],
            })
            mlflow.spark.log_model(
                model,
                name="model",
            )

            results.append(
                CandidateResult(
                    name=name,
                    validation_metrics=metrics,
                    run_id=run.info.run_id,
                )
            )

    return results


def promote_selected_candidate(
    *,
    selected: CandidateResult | None,
    client: MlflowClient,
    model_name: str,
    register_model: Callable = mlflow.register_model,
    load_model: Callable = mlflow.spark.load_model,
    champion_lookup: Callable = get_champion_version,
) -> str:
    if selected is None:
        existing = champion_lookup(client, model_name)
        if existing is None:
            raise RuntimeError(
                "No eligible forecast model and no existing champion."
            )
        return str(existing.version)

    registered = register_model(
        f"runs:/{selected.run_id}/model",
        model_name,
    )
    version = str(registered.version)

    load_model(
        f"models:/{model_name}/{version}"
    )

    client.set_registered_model_alias(
        model_name,
        "champion",
        version,
    )

    return version
