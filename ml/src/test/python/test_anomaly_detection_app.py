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

    def test_build_anomaly_candidates_matches_actual_with_forecast(self):
        actual = self.spark.createDataFrame(
            [("SKU-001", "2026-10-01", 120.0)],
            ["productId", "eventDate", "unitsSold"],
        )

        forecast = self.spark.createDataFrame(
            [("SKU-001", "2026-10-01", 100.0, "7")],
            [
                "product_id",
                "forecast_date",
                "predicted_units",
                "model_version",
            ],
        )

        result = build_anomaly_candidates(
            actual=actual,
            forecast=forecast,
        ).collect()

        self.assertEqual(1, len(result))
        self.assertEqual("SKU-001", result[0]["product_id"])
        self.assertEqual(120.0, result[0]["actual_units"])
        self.assertEqual(100.0, result[0]["expected_units"])
        self.assertEqual(20.0, result[0]["residual"])

    def test_build_anomaly_candidates_excludes_rows_without_matching_forecast(self):
        actual = self.spark.createDataFrame(
            [
                ("SKU-001", "2026-10-01", 120.0),
                ("SKU-002", "2026-10-01", 80.0),
            ],
            ["productId", "eventDate", "unitsSold"],
        )

        forecast = self.spark.createDataFrame(
            [("SKU-001", "2026-10-01", 100.0, "7")],
            [
                "product_id",
                "forecast_date",
                "predicted_units",
                "model_version",
            ],
        )

        result = build_anomaly_candidates(
            actual=actual,
            forecast=forecast,
        ).collect()

        self.assertEqual(1, len(result))
        self.assertEqual("SKU-001", result[0]["product_id"])

    def test_build_anomaly_candidates_calculates_residual_per_product(self):
        actual = self.spark.createDataFrame(
            [
                ("SKU-001", "2026-10-01", 120.0),
                ("SKU-002", "2026-10-01", 80.0),
            ],
            ["productId", "eventDate", "unitsSold"],
        )

        forecast = self.spark.createDataFrame(
            [
                ("SKU-001", "2026-10-01", 100.0, "7"),
                ("SKU-002", "2026-10-01", 100.0, "7"),
            ],
            [
                "product_id",
                "forecast_date",
                "predicted_units",
                "model_version",
            ],
        )

        result = (
            build_anomaly_candidates(
                actual=actual,
                forecast=forecast,
            )
            .orderBy("product_id")
            .collect()
        )

        self.assertEqual(2, len(result))
        self.assertEqual("SKU-001", result[0]["product_id"])
        self.assertEqual(20.0, result[0]["residual"])
        self.assertEqual("SKU-002", result[1]["product_id"])
        self.assertEqual(-20.0, result[1]["residual"])

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

        self.assertEqual(2, len(result))

        sku_001 = result[0]
        sku_002 = result[1]

        self.assertEqual("SKU-001", sku_001["product_id"])
        self.assertEqual(
            [1.0, 2.0],
            sorted(sku_001["historical_residuals"]),
        )
        self.assertNotIn(100.0, sku_001["historical_residuals"])

        self.assertEqual("SKU-002", sku_002["product_id"])
        self.assertEqual(
            [10.0, 20.0],
            sorted(sku_002["historical_residuals"]),
        )
        self.assertNotIn(30.0, sku_002["historical_residuals"])

    def test_score_anomaly_candidates_returns_staging_contract(self):
        candidates = self.spark.createDataFrame(
            [
                (
                    "2026-10-04",
                    "SKU-001",
                    150.0,
                    100.0,
                    50.0,
                    "7",
                    [-2.0, 0.0, 2.0],
                ),
            ],
            [
                "event_date",
                "product_id",
                "actual_units",
                "expected_units",
                "residual",
                "model_version",
                "historical_residuals",
            ],
        )

        row = score_anomaly_candidates(candidates).collect()[0]

        self.assertEqual("SKU-001", row["product_id"])
        self.assertEqual(150.0, row["actual_units"])
        self.assertEqual(100.0, row["expected_units"])
        self.assertEqual(50.0, row["residual"])
        self.assertAlmostEqual(16.8625, row["anomaly_score"], places=4)
        self.assertTrue(row["is_anomaly"])
        self.assertEqual("7", row["model_version"])
        self.assertIsNotNone(row["detected_at"])

    def test_stage_anomaly_results_uses_sales_anomaly_staging_table(self):
        scored = self.spark.createDataFrame(
            [
                (
                    "2026-10-04",
                    "SKU-001",
                    150.0,
                    100.0,
                    50.0,
                    16.8625,
                    True,
                    "7",
                )
            ],
            [
                "event_date",
                "product_id",
                "actual_units",
                "expected_units",
                "residual",
                "anomaly_score",
                "is_anomaly",
                "model_version",
            ],
        )

        captured = {}

        def fake_stage(df, staging_table, jdbc_options):
            captured["columns"] = df.columns
            captured["staging_table"] = staging_table
            captured["jdbc_options"] = jdbc_options

        jdbc_options = {
            "url": "jdbc:postgresql://postgres:5432/smartretail",
            "user": "smartretail",
            "password": "smartretail",
            "driver": "org.postgresql.Driver",
            "pg_dsn": "dbname=smartretail user=smartretail host=postgres",
        }

        stage_anomaly_results(
            scored,
            jdbc_options=jdbc_options,
            stage_fn=fake_stage,
        )

        self.assertEqual(
            "analytics.sales_anomaly_staging",
            captured["staging_table"],
        )
        self.assertEqual(jdbc_options, captured["jdbc_options"])


if __name__ == "__main__":
    unittest.main()
