import os
import sys
import unittest

from types import SimpleNamespace
from unittest.mock import Mock, patch

from mlflow.exceptions import MlflowException
from mlflow.protos.databricks_pb2 import RESOURCE_DOES_NOT_EXIST


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_PYTHON_DIR = os.path.abspath(
    os.path.join(CURRENT_DIR, "..", "..", "main", "python")
)
sys.path.insert(0, MAIN_PYTHON_DIR)

from mlflow_support import configure_mlflow, get_champion_version


class MlflowSupportTest(unittest.TestCase):

    @patch("mlflow_support.MlflowClient")
    @patch("mlflow_support.mlflow.set_tracking_uri")
    def test_configure_mlflow_reuses_existing_experiment(
        self,
        set_tracking_uri,
        client_factory,
    ):
        client = client_factory.return_value
        client.get_experiment_by_name.return_value = SimpleNamespace(
            experiment_id="17"
        )

        experiment_id = configure_mlflow(
            "http://mlflow:5000",
            "smartretail-demand-forecast",
        )

        self.assertEqual(experiment_id, "17")
        set_tracking_uri.assert_called_once_with("http://mlflow:5000")
        client.get_experiment_by_name.assert_called_once_with(
            "smartretail-demand-forecast"
        )
        client.create_experiment.assert_not_called()

    @patch("mlflow_support.MlflowClient")
    @patch("mlflow_support.mlflow.set_tracking_uri")
    def test_configure_mlflow_creates_missing_experiment(
        self,
        set_tracking_uri,
        client_factory,
    ):
        client = client_factory.return_value
        client.get_experiment_by_name.return_value = None
        client.create_experiment.return_value = "23"

        experiment_id = configure_mlflow(
            "http://mlflow:5000",
            "smartretail-demand-forecast",
        )

        self.assertEqual(experiment_id, "23")
        client.create_experiment.assert_called_once_with(
            "smartretail-demand-forecast"
        )

    def test_get_champion_version_returns_alias_version(self):
        client = Mock()
        expected = SimpleNamespace(version="4")
        client.get_model_version_by_alias.return_value = expected

        actual = get_champion_version(
            client,
            "smartretail-demand-forecast",
        )

        self.assertIs(actual, expected)
        client.get_model_version_by_alias.assert_called_once_with(
            "smartretail-demand-forecast",
            "champion",
        )

    def test_get_champion_version_returns_none_when_alias_is_missing(self):
        client = Mock()
        client.get_model_version_by_alias.side_effect = MlflowException(
            "champion alias does not exist",
            error_code=RESOURCE_DOES_NOT_EXIST,
        )

        actual = get_champion_version(
            client,
            "smartretail-demand-forecast",
        )

        self.assertIsNone(actual)

    def test_get_champion_version_reraises_non_missing_errors(self):
        client = Mock()
        client.get_model_version_by_alias.side_effect = MlflowException(
            "tracking server unavailable"
        )

        with self.assertRaises(MlflowException):
            get_champion_version(
                client,
                "smartretail-demand-forecast",
            )


if __name__ == "__main__":
    unittest.main()
