import os
import sys
import unittest

from types import SimpleNamespace
from unittest.mock import Mock, patch


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_PYTHON_DIR = os.path.abspath(
    os.path.join(CURRENT_DIR, "..", "..", "main", "python")
)
sys.path.insert(0, MAIN_PYTHON_DIR)

import champion_validation_app


class ChampionValidationAppTest(unittest.TestCase):

    @patch("champion_validation_app.mlflow.spark.load_model")
    @patch("champion_validation_app.get_champion_version")
    @patch("champion_validation_app.MlflowClient")
    @patch("champion_validation_app.mlflow.set_tracking_uri")
    @patch("champion_validation_app.configure_mlflow")
    @patch("champion_validation_app.build_spark_session")
    def test_main_initializes_spark_before_loading_champion(
        self,
        build_spark_session,
        configure_mlflow,
        set_tracking_uri,
        client_factory,
        get_champion_version,
        load_model,
    ):
        spark = Mock()
        build_spark_session.return_value = spark
        get_champion_version.return_value = SimpleNamespace(version="1")

        champion_validation_app.main()

        build_spark_session.assert_called_once_with()
        spark.sparkContext.setLogLevel.assert_called_once_with("WARN")
        configure_mlflow.assert_called_once()
        set_tracking_uri.assert_called_once()
        client_factory.assert_called_once_with()
        get_champion_version.assert_called_once()
        load_model.assert_called_once_with(
            "models:/smartretail-demand-forecast@champion"
        )
        spark.stop.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
