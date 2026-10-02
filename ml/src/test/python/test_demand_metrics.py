import math
import os
import sys
import unittest

from pyspark.sql import SparkSession


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_PYTHON_DIR = os.path.abspath(
    os.path.join(CURRENT_DIR, "..", "..", "main", "python")
)
sys.path.insert(0, MAIN_PYTHON_DIR)

from demand_metrics import (
    build_seasonal_naive_predictions,
    evaluate_regression,
)


class DemandMetricsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("smartretail-demand-metrics-test")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_evaluate_regression_returns_mae_rmse_and_wape(self):
        predictions = self.spark.createDataFrame(
            [
                (10.0, 8.0),
                (20.0, 22.0),
                (30.0, 27.0),
            ],
            ["unitsSold", "prediction"],
        )

        metrics = evaluate_regression(predictions)

        self.assertAlmostEqual(metrics["mae"], 7.0 / 3.0, places=6)
        self.assertAlmostEqual(
            metrics["rmse"],
            math.sqrt(17.0 / 3.0),
            places=6,
        )
        self.assertAlmostEqual(metrics["wape"], 7.0 / 60.0, places=6)

    def test_seasonal_naive_uses_lag7_as_prediction(self):
        features = self.spark.createDataFrame(
            [
                (15, 11),
                (21, 19),
            ],
            ["unitsSold", "lag7"],
        )

        result = build_seasonal_naive_predictions(features).collect()

        self.assertEqual(result[0]["prediction"], 11.0)
        self.assertEqual(result[1]["prediction"], 19.0)


if __name__ == "__main__":
    unittest.main()
