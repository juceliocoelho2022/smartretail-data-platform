import os
import sys
import unittest

from datetime import date, timedelta

from pyspark.sql import SparkSession


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_PYTHON_DIR = os.path.abspath(
    os.path.join(CURRENT_DIR, "..", "..", "main", "python")
)
sys.path.insert(0, MAIN_PYTHON_DIR)

from demand_features import build_demand_features, chronological_split


class DemandFeaturesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("smartretail-demand-features-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def known_sequence(self, days=100):
        start = date(2026, 1, 1)
        rows = [
            (start + timedelta(days=index), "PROD-001", index + 1)
            for index in range(days)
        ]
        return self.spark.createDataFrame(
            rows,
            ["eventDate", "productId", "unitsSold"],
        )

    def test_lags_and_rolling_windows_use_only_prior_targets(self):
        features = build_demand_features(self.known_sequence(40))
        target_date = date(2026, 2, 4)

        row = (
            features
            .filter(features.eventDate == target_date)
            .first()
        )

        self.assertEqual(row["unitsSold"], 35)
        self.assertEqual(row["lag1"], 34)
        self.assertEqual(row["lag7"], 28)
        self.assertEqual(row["lag14"], 21)
        self.assertAlmostEqual(row["rollingMean7"], 31.0, places=6)
        self.assertAlmostEqual(row["rollingMean28"], 20.5, places=6)

    def test_required_history_features_are_never_null(self):
        features = build_demand_features(self.known_sequence(40))

        self.assertEqual(
            features.filter(
                "lag1 IS NULL OR lag7 IS NULL OR lag14 IS NULL "
                "OR rollingMean7 IS NULL OR rollingMean28 IS NULL "
                "OR rollingStd7 IS NULL"
            ).count(),
            0,
        )
        self.assertEqual(features.count(), 12)

    def test_chronological_split_uses_global_final_56_days(self):
        features = build_demand_features(self.known_sequence(100))
        train, validation, test = chronological_split(features)

        train_dates = {
            row["eventDate"]
            for row in train.select("eventDate").distinct().collect()
        }
        validation_dates = {
            row["eventDate"]
            for row in validation.select("eventDate").distinct().collect()
        }
        test_dates = {
            row["eventDate"]
            for row in test.select("eventDate").distinct().collect()
        }

        self.assertEqual(len(validation_dates), 28)
        self.assertEqual(len(test_dates), 28)
        self.assertTrue(train_dates)
        self.assertTrue(train_dates.isdisjoint(validation_dates))
        self.assertTrue(train_dates.isdisjoint(test_dates))
        self.assertTrue(validation_dates.isdisjoint(test_dates))
        self.assertLess(max(train_dates), min(validation_dates))
        self.assertLess(max(validation_dates), min(test_dates))
        self.assertEqual(max(test_dates), date(2026, 4, 10))


if __name__ == "__main__":
    unittest.main()
