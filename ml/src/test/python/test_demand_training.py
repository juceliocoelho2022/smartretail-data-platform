import math
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

from demand_training import (
    CandidateResult,
    promote_selected_candidate,
    select_eligible_champion,
    train_candidate_runs,
)


class DemandTrainingPolicyTest(unittest.TestCase):

    def candidate(self, name, mae, run_id="run-1"):
        return CandidateResult(
            name=name,
            validation_metrics={
                "mae": mae,
                "rmse": 2.0,
                "wape": 0.1,
            },
            run_id=run_id,
        )

    def test_lowest_finite_candidate_strictly_below_baseline_wins(self):
        selected = select_eligible_champion(
            [
                self.candidate("random_forest", 4.0, "rf"),
                self.candidate("gbt", 3.0, "gbt"),
            ],
            baseline_mae=5.0,
        )

        self.assertIsNotNone(selected)
        self.assertEqual(selected.name, "gbt")
        self.assertEqual(selected.run_id, "gbt")

    def test_equal_or_worse_candidate_is_not_eligible(self):
        selected = select_eligible_champion(
            [
                self.candidate("random_forest", 5.0),
                self.candidate("gbt", 6.0),
            ],
            baseline_mae=5.0,
        )

        self.assertIsNone(selected)

    def test_non_finite_metrics_are_rejected(self):
        nan_candidate = CandidateResult(
            "random_forest",
            {"mae": math.nan, "rmse": 2.0, "wape": 0.1},
            "nan-run",
        )
        inf_candidate = CandidateResult(
            "gbt",
            {"mae": 2.0, "rmse": math.inf, "wape": 0.1},
            "inf-run",
        )

        self.assertIsNone(
            select_eligible_champion(
                [nan_candidate, inf_candidate],
                baseline_mae=5.0,
            )
        )

    @patch("demand_training.mlflow.spark.log_model")
    @patch("demand_training.mlflow.log_metrics")
    @patch("demand_training.mlflow.log_params")
    @patch("demand_training.mlflow.start_run")
    @patch("demand_training.evaluate_regression")
    @patch("demand_training.build_candidate_pipelines")
    def test_training_logs_spark_model_at_run_relative_artifact_path(
        self,
        build_candidate_pipelines,
        evaluate_regression,
        start_run,
        log_params,
        log_metrics,
        log_model,
    ):
        pipeline = Mock()
        model = Mock()
        predictions = Mock()
        pipeline.fit.return_value = model
        model.transform.return_value = predictions
        build_candidate_pipelines.return_value = {
            "random_forest": pipeline
        }
        evaluate_regression.return_value = {
            "mae": 1.0,
            "rmse": 1.5,
            "wape": 0.1,
        }
        start_run.return_value.__enter__.return_value = SimpleNamespace(
            info=SimpleNamespace(run_id="rf-run")
        )

        results = train_candidate_runs(
            Mock(),
            Mock(),
            baseline_metrics={
                "mae": 2.0,
                "rmse": 2.5,
                "wape": 0.2,
            },
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].run_id, "rf-run")
        log_model.assert_called_once_with(
            model,
            artifact_path="model",
        )

    def test_existing_champion_is_untouched_when_no_candidate_qualifies(self):
        client = Mock()
        client.get_model_version_by_alias.return_value = SimpleNamespace(
            version="7"
        )
        register_model = Mock()
        load_model = Mock()

        version = promote_selected_candidate(
            selected=None,
            client=client,
            model_name="smartretail-demand-forecast",
            register_model=register_model,
            load_model=load_model,
        )

        self.assertEqual(version, "7")
        register_model.assert_not_called()
        load_model.assert_not_called()
        client.set_registered_model_alias.assert_not_called()

    def test_first_run_without_eligible_candidate_raises(self):
        client = Mock()
        client.get_model_version_by_alias.side_effect = Exception(
            "missing champion"
        )

        with self.assertRaises(RuntimeError):
            promote_selected_candidate(
                selected=None,
                client=client,
                model_name="smartretail-demand-forecast",
                register_model=Mock(),
                load_model=Mock(),
                champion_lookup=lambda _client, _model_name: None,
            )

    def test_alias_is_assigned_only_after_registered_model_loads(self):
        selected = self.candidate("gbt", 3.0, "gbt-run")
        client = Mock()
        register_model = Mock(
            return_value=SimpleNamespace(version="11")
        )
        load_model = Mock()

        version = promote_selected_candidate(
            selected=selected,
            client=client,
            model_name="smartretail-demand-forecast",
            register_model=register_model,
            load_model=load_model,
            champion_lookup=lambda _client, _model_name: None,
        )

        self.assertEqual(version, "11")
        register_model.assert_called_once_with(
            "runs:/gbt-run/model",
            "smartretail-demand-forecast",
        )
        load_model.assert_called_once_with(
            "models:/smartretail-demand-forecast/11"
        )
        client.set_registered_model_alias.assert_called_once_with(
            "smartretail-demand-forecast",
            "champion",
            "11",
        )

    def test_alias_is_not_assigned_when_model_validation_fails(self):
        selected = self.candidate("gbt", 3.0, "gbt-run")
        client = Mock()
        register_model = Mock(
            return_value=SimpleNamespace(version="11")
        )
        load_model = Mock(side_effect=RuntimeError("cannot load"))

        with self.assertRaises(RuntimeError):
            promote_selected_candidate(
                selected=selected,
                client=client,
                model_name="smartretail-demand-forecast",
                register_model=register_model,
                load_model=load_model,
                champion_lookup=lambda _client, _model_name: None,
            )

        client.set_registered_model_alias.assert_not_called()


if __name__ == "__main__":
    unittest.main()
