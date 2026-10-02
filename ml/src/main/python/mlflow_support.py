import mlflow

from mlflow import MlflowClient
from mlflow.entities.model_registry import ModelVersion
from mlflow.exceptions import MlflowException
from mlflow.protos.databricks_pb2 import (
    ErrorCode,
    RESOURCE_DOES_NOT_EXIST,
)


_MISSING_RESOURCE = ErrorCode.Name(
    RESOURCE_DOES_NOT_EXIST
)


def configure_mlflow(
    tracking_uri: str,
    experiment_name: str,
) -> str:
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()

    experiment = client.get_experiment_by_name(
        experiment_name
    )
    if experiment is not None:
        return experiment.experiment_id

    return client.create_experiment(
        experiment_name
    )


def get_champion_version(
    client: MlflowClient,
    model_name: str,
) -> ModelVersion | None:
    try:
        return client.get_model_version_by_alias(
            model_name,
            "champion",
        )
    except MlflowException as exc:
        if exc.error_code == _MISSING_RESOURCE:
            return None
        raise
