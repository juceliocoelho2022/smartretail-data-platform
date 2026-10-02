import os
import sys
import unittest

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


CURRENT_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

MAIN_PYTHON_DIR = os.path.abspath(
    os.path.join(
        CURRENT_DIR,
        "..",
        "..",
        "main",
        "python",
    )
)

sys.path.insert(0, MAIN_PYTHON_DIR)

from historical_data_generator import generate_silver_history


class HistoricalDataGeneratorTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("smartretail-ml-history-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def build_history(self):
        return generate_silver_history(
            self.spark,
            date(2025, 10, 1),
            365,
            30,
            20261002,
        )

    def test_generation_is_deterministic_for_same_seed(self):
        first = self.build_history()
        second = self.build_history()

        self.assertEqual(first.count(), second.count())
        self.assertEqual(
            first.orderBy("eventId").collect(),
            second.orderBy("eventId").collect(),
        )

    def test_generation_covers_30_products_and_365_dates(self):
        history = self.build_history()

        self.assertEqual(
            history.select("productId").distinct().count(),
            30,
        )
        self.assertEqual(
            history.select("eventDate").distinct().count(),
            365,
        )

    def test_generation_emits_only_valid_silver_values(self):
        history = self.build_history()

        invalid = history.filter(
            (F.col("quantity") <= 0)
            | (F.col("unitPrice") < 0)
            | F.col("eventId").isNull()
            | F.col("customerId").isNull()
            | F.col("productId").isNull()
        )

        self.assertEqual(invalid.count(), 0)
        self.assertEqual(
            history.select("eventId").distinct().count(),
            history.count(),
        )

    def test_controlled_spike_exists_in_final_28_days(self):
        history = self.build_history()

        spike_units = (
            history
            .filter(F.col("productId") == "PROD-001")
            .filter(F.col("eventDate") == F.lit("2026-09-30"))
            .agg(F.sum("quantity").alias("units"))
            .first()["units"]
        )

        self.assertIsNotNone(spike_units)
        self.assertGreaterEqual(spike_units, 50)


if __name__ == "__main__":
    unittest.main()
