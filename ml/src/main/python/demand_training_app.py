import argparse
import json
import math

import mlflow
import mlflow.spark
from mlflow import MlflowClient
from pyspark.sql import SparkSession

from demand_features import chronological_split
from demand_metrics import (
    build_seasonal_naive_predictions,
    evaluate_regression,
)
from demand_training import (
    CandidateResult,
    promote_selected_candidate,
    select_eligible_champion,
    train_candidate_runs,
)
from ml_config import (
    ML_FEATURES_PATH,
    MLFLOW_TRACKING_URI,
    ML_TRAINING_MANIFEST_PATH,
    MODEL_NAME,
    S3_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_KEY,
)
from mlflow_support import configure_mlflow


EXPERIMENT_NAME = "smartretail-demand-forecast"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("smartretail-demand-training")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint.region", S3_REGION)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )


def _manifest_to_json(
    baseline_metrics: dict[str, float],
    candidates: list[CandidateResult],
) -> str:
    return json.dumps(
        {
            "baselineMetrics": baseline_metrics,
            "candidates": [
                {
                    "name": candidate.name,
                    "validationMetrics": candidate.validation_metrics,
                    "runId": candidate.run_id,
                }
                for candidate in candidates
            ],
        },
        sort_keys=True,
    )


def _read_manifest(spark: SparkSession) -> dict:
    rows = spark.read.text(
        ML_TRAINING_MANIFEST_PATH
    ).collect()
    if len(rows) != 1:
        raise RuntimeError(
            "Training manifest must contain exactly one JSON record."
        )
    return json.loads(rows[0]["value"])


def _candidates_from_manifest(manifest: dict) -> list[CandidateResult]:
    return [
        CandidateResult(
            name=item["name"],
            validation_metrics=item["validationMetrics"],
            run_id=item["runId"],
        )
        for item in manifest["candidates"]
    ]


def train_phase(spark: SparkSession) -> None:
    configure_mlflow(
        MLFLOW_TRACKING_URI,
        EXPERIMENT_NAME,
    )
    mlflow.set_experiment(EXPERIMENT_NAME)

    features = spark.read.parquet(ML_FEATURES_PATH)
    train, validation, _ = chronological_split(features)

    if train.rdd.isEmpty() or validation.rdd.isEmpty():
        raise RuntimeError(
            "Training and validation datasets must not be empty."
        )

    baseline_predictions = build_seasonal_naive_predictions(
        validation
    )
    baseline_metrics = evaluate_regression(
        baseline_predictions
    )

    candidates = train_candidate_runs(
        train,
        validation,
        baseline_metrics,
    )

    manifest_json = _manifest_to_json(
        baseline_metrics,
        candidates,
    )
    (
        spark.createDataFrame(
            [(manifest_json,)],
            ["value"],
        )
        .coalesce(1)
        .write
        .mode("overwrite")
        .text(ML_TRAINING_MANIFEST_PATH)
    )

    print("Demand model training: PASSED")
    print(f"Baseline validation MAE={baseline_metrics['mae']}")
    for candidate in candidates:
        print(
            f"Candidate={candidate.name} "
            f"validationMAE={candidate.validation_metrics['mae']} "
            f"runId={candidate.run_id}"
        )


def evaluate_register_phase(spark: SparkSession) -> None:
    configure_mlflow(
        MLFLOW_TRACKING_URI,
        EXPERIMENT_NAME,
    )

    manifest = _read_manifest(spark)
    candidates = _candidates_from_manifest(manifest)
    baseline_mae = float(
        manifest["baselineMetrics"]["mae"]
    )
    selected = select_eligible_champion(
        candidates,
        baseline_mae,
    )

    features = spark.read.parquet(ML_FEATURES_PATH)
    _, _, test = chronological_split(features)
    client = MlflowClient()

    if selected is not None:
        candidate_model = mlflow.spark.load_model(
            f"runs:/{selected.run_id}/model"
        )
        test_metrics = evaluate_regression(
            candidate_model.transform(test)
        )
        if not all(
            math.isfinite(float(test_metrics[key]))
            for key in ("mae", "rmse", "wape")
        ):
            raise RuntimeError(
                "Selected model produced non-finite test metrics."
            )

        for key, value in test_metrics.items():
            client.log_metric(
                selected.run_id,
                f"test_{key}",
                float(value),
            )

    champion_version = promote_selected_candidate(
        selected=selected,
        client=client,
        model_name=MODEL_NAME,
    )

    print("Demand champion registration: PASSED")
    print(f"Model={MODEL_NAME}")
    print(f"Champion version={champion_version}")
    if selected is not None:
        print(f"Selected candidate={selected.name}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--phase",
        required=True,
        choices=("train", "evaluate-register"),
    )
    args = parser.parse_args()

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        if args.phase == "train":
            train_phase(spark)
        else:
            evaluate_register_phase(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
