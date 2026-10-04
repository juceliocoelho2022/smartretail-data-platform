import os
import sys
import unittest

from pyspark.sql import SparkSession

PROJECT_ROOT = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "main",
        "python",
    )
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from anomaly_detection_app import (
    attach_residual_history,
    build_anomaly_candidates,
    score_anomaly_candidates,
    select_temporally_valid_forecasts,
    stage_anomaly_results,
)


class AnomalyDetectionAppTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("anomaly-detection-test")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_select_temporally_valid_forecasts_rejects_same_day_and_future_cutoffs(self):
        forecast = self.spark.createDataFrame(
            [
                ("SKU-001", "2026-10-10", 100.0, "7", "2026-10-09", "2026-10-09 10:00:00"),
                ("SKU-001", "2026-10-10", 101.0, "7", "2026-10-10", "2026-10-10 08:00:00"),
                ("SKU-001", "2026-10-10", 102.0, "7", "2026-10-11", "2026-10-11 08:00:00"),
            ],
            ["product_id", "forecast_date", "predicted_units", "model_version", "training_cutoff_date", "generated_at"],
        )

        rows = select_temporally_valid_forecasts(forecast).collect()

        self.assertEqual(1, len(rows))
        self.assertEqual(100.0, rows[0]["predicted_units"])

    def test_select_temporally_valid_forecasts_chooses_latest_cutoff_then_generated_at(self):
        forecast = self.spark.createDataFrame(
            [
                ("SKU-001", "2026-10-10", 90.0, "7", "2026-10-07", "2026-10-07 12:00:00"),
                ("SKU-001", "2026-10-10", 95.0, "7", "2026-10-09", "2026-10-09 08:00:00"),
                ("SKU-001", "2026-10-10", 97.0, "8", "2026-10-09", "2026-10-09 18:00:00"),
            ],
            ["product_id", "forecast_date", "predicted_units", "model_version", "training_cutoff_date", "generated_at"],
        )

        rows = select_temporally_valid_forecasts(forecast).collect()

        self.assertEqual(1, len(rows))
        self.assertEqual(97.0, rows[0]["predicted_units"])
        self.assertEqual("8", rows[0]["model_version"])

    def test_build_anomaly_candidates_matches_actual_with_forecast(self):
        actual = self.spark.createDataFrame(
            [("SKU-001", "2026-10-01", 120.0)],
            ["productId", "eventDate", "unitsSold"],
        )
        forecast = self.spark.createDataFrame(
            [("SKU-001", "2026-10-01", 100.0, "7")],
            ["product_id", "forecast_date", "predicted_units", "model_version"],
        )

        result = build_anomaly_candidates(actual=actual, forecast=forecast).collect()

        self.assertEqual(1, len(result))
        self.assertEqual(20.0, result[0]["residual"])

    def test_attach_residual_history_uses_only_previous_rows_of_same_product(self):
        candidates = self.spark.createDataFrame(
            [
                ("2026-10-01", "SKU-001", 1.0),
                ("2026-10-02", "SKU-001", 2.0),
                ("2026-10-03", "SKU-001", 100.0),
                ("2026-10-01", "SKU-002", 10.0),
                ("2026-10-02", "SKU-002", 20.0),
                ("2026-10-03", "SKU-002", 30.0),
            ],
            ["event_date", "product_id", "residual"],
        )

        result = (
            attach_residual_history(candidates)
            .filter("event_date = '2026-10-03'")
            .orderBy("product_id")
            .collect()
        )

        self.assertEqual([1.0, 2.0], sorted(result[0]["historical_residuals"]))
        self.assertNotIn(100.0, result[0]["historical_residuals"])
        self.assertEqual([10.0, 20.0], sorted(result[1]["historical_residuals"]))
        self.assertNotIn(30.0, result[1]["historical_residuals"])

    def test_attach_residual_history_keeps_only_previous_28_rows(self):
        candidates = self.spark.createDataFrame(
            [(f"2026-09-{day:02d}", "SKU-001", float(day)) for day in range(1, 31)],
            ["event_date", "product_id", "residual"],
        )

        row = (
            attach_residual_history(candidates)
            .filter("event_date = '2026-09-30'")
            .collect()[0]
        )

        self.assertEqual(28, len(row["historical_residuals"]))
        self.assertNotIn(1.0, row["historical_residuals"])
        self.assertNotIn(30.0, row["historical_residuals"])

    def test_score_anomaly_candidates_requires_seven_prior_residuals(self):
        rows = []
        for size in range(7):
            rows.append(("2026-10-04", f"SKU-{size}", 110.0, 100.0, 10.0, "7", [float(v) for v in range(size)]))
        rows.append(("2026-10-04", "SKU-7", 110.0, 100.0, 10.0, "7", [float(v) for v in range(7)]))

        candidates = self.spark.createDataFrame(
            rows,
            ["event_date", "product_id", "actual_units", "expected_units", "residual", "model_version", "historical_residuals"],
        )

        result = score_anomaly_candidates(candidates).collect()

        self.assertEqual(1, len(result))
        self.assertEqual("SKU-7", result[0]["product_id"])

    def test_score_anomaly_candidates_returns_staging_contract(self):
        candidates = self.spark.createDataFrame(
            [("2026-10-04", "SKU-001", 150.0, 100.0, 50.0, "7", [-3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0])],
            ["event_date", "product_id", "actual_units", "expected_units", "residual", "model_version", "historical_residuals"],
        )

        row = score_anomaly_candidates(candidates).collect()[0]

        self.assertAlmostEqual(16.8625, row["anomaly_score"], places=4)
        self.assertTrue(row["is_anomaly"])
        self.assertIsNotNone(row["detected_at"])

    def test_stage_anomaly_results_uses_sales_anomaly_staging_table(self):
        scored = self.spark.createDataFrame(
            [("2026-10-04", "SKU-001", 150.0, 100.0, 50.0, 16.8625, True, "7")],
            ["event_date", "product_id", "actual_units", "expected_units", "residual", "anomaly_score", "is_anomaly", "model_version"],
        )
        captured = {}

        def fake_stage(df, staging_table, jdbc_options):
            captured["staging_table"] = staging_table
            captured["jdbc_options"] = jdbc_options

        jdbc_options = {"pg_dsn": "dsn"}
        stage_anomaly_results(scored, jdbc_options=jdbc_options, stage_fn=fake_stage)

        self.assertEqual("analytics.sales_anomaly_staging", captured["staging_table"])
        self.assertEqual(jdbc_options, captured["jdbc_options"])


if __name__ == "__main__":
    unittest.main()
