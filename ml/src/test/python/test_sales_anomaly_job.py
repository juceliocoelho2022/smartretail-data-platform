import os
import sys
import unittest
from unittest.mock import MagicMock, patch


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_PYTHON_DIR = os.path.abspath(
    os.path.join(CURRENT_DIR, "..", "..", "main", "python")
)
if MAIN_PYTHON_DIR not in sys.path:
    sys.path.insert(0, MAIN_PYTHON_DIR)

from sales_anomaly_job import run_sales_anomaly_job


class SalesAnomalyJobTest(unittest.TestCase):

    @patch("sales_anomaly_job.merge_from_staging")
    @patch("sales_anomaly_job.stage_anomaly_results")
    @patch("sales_anomaly_job.score_anomaly_candidates")
    @patch("sales_anomaly_job.attach_residual_history")
    @patch("sales_anomaly_job.build_anomaly_candidates")
    @patch("sales_anomaly_job.select_temporally_valid_forecasts")
    @patch("sales_anomaly_job.load_forecasts")
    @patch("sales_anomaly_job.load_actuals")
    def test_run_job_orders_pipeline_stages_merges_and_returns_metrics(
        self,
        load_actuals,
        load_forecasts,
        select_forecasts,
        build_candidates,
        attach_history,
        score_candidates,
        stage_results,
        merge,
    ):
        spark = MagicMock()
        jdbc_options = {
            "url": "jdbc:postgresql://postgres:5432/smartretail",
            "user": "smartretail",
            "password": "smartretail",
            "driver": "org.postgresql.Driver",
            "pg_dsn": "dbname=smartretail user=smartretail password=smartretail host=postgres port=5432",
        }

        actual = MagicMock(name="actual")
        forecast = MagicMock(name="forecast")
        selected = MagicMock(name="selected")
        candidates = MagicMock(name="candidates")
        history = MagicMock(name="history")
        scored = MagicMock(name="scored")
        anomalies = MagicMock(name="anomalies")

        load_actuals.return_value = actual
        load_forecasts.return_value = forecast
        select_forecasts.return_value = selected
        build_candidates.return_value = candidates
        attach_history.return_value = history
        score_candidates.return_value = scored
        candidates.count.return_value = 12
        scored.count.return_value = 5
        scored.filter.return_value = anomalies
        anomalies.count.return_value = 2

        metrics = run_sales_anomaly_job(spark, jdbc_options)

        load_actuals.assert_called_once_with(spark)
        load_forecasts.assert_called_once_with(spark, jdbc_options)
        select_forecasts.assert_called_once_with(forecast)
        build_candidates.assert_called_once_with(actual=actual, forecast=selected)
        attach_history.assert_called_once_with(candidates)
        score_candidates.assert_called_once_with(history)
        stage_results.assert_called_once_with(scored, jdbc_options=jdbc_options)
        merge.assert_called_once()
        self.assertEqual(
            {"candidate_count": 12, "scored_count": 5, "anomaly_count": 2},
            metrics,
        )

    @patch("sales_anomaly_job.merge_from_staging")
    @patch("sales_anomaly_job.stage_anomaly_results")
    @patch("sales_anomaly_job.score_anomaly_candidates")
    @patch("sales_anomaly_job.attach_residual_history")
    @patch("sales_anomaly_job.build_anomaly_candidates")
    @patch("sales_anomaly_job.select_temporally_valid_forecasts")
    @patch("sales_anomaly_job.load_forecasts")
    @patch("sales_anomaly_job.load_actuals")
    def test_run_job_uses_non_destructive_anomaly_merge_contract(
        self,
        load_actuals,
        load_forecasts,
        select_forecasts,
        build_candidates,
        attach_history,
        score_candidates,
        stage_results,
        merge,
    ):
        load_actuals.return_value = MagicMock()
        load_forecasts.return_value = MagicMock()
        select_forecasts.return_value = MagicMock()
        candidates = MagicMock()
        candidates.count.return_value = 0
        build_candidates.return_value = candidates
        attach_history.return_value = MagicMock()
        scored = MagicMock()
        scored.count.return_value = 0
        scored.filter.return_value.count.return_value = 0
        score_candidates.return_value = scored

        run_sales_anomaly_job(MagicMock(), {"pg_dsn": "dsn"})

        merge.assert_called_once_with(
            target_table="analytics.sales_anomaly",
            staging_table="analytics.sales_anomaly_staging",
            ordered_columns=[
                "event_date",
                "product_id",
                "actual_units",
                "expected_units",
                "residual",
                "anomaly_score",
                "is_anomaly",
                "model_version",
                "detected_at",
            ],
            conflict_columns=["event_date", "product_id", "model_version"],
            pg_dsn="dsn",
        )


if __name__ == "__main__":
    unittest.main()
