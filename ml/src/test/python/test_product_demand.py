import os
import sys
import unittest

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_PYTHON_DIR = os.path.abspath(
    os.path.join(CURRENT_DIR, "..", "..", "main", "python")
)
sys.path.insert(0, MAIN_PYTHON_DIR)

from product_demand import build_product_demand_daily


SILVER_SCHEMA = StructType([
    StructField("eventId", StringType(), False),
    StructField("occurredAt", TimestampType(), False),
    StructField("customerId", StringType(), False),
    StructField("productId", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unitPrice", DecimalType(19, 2), False),
    StructField("channel", StringType(), False),
    StructField("location", StringType(), False),
    StructField("revenue", DecimalType(19, 2), False),
    StructField("eventDate", DateType(), False),
    StructField("silverProcessedAt", TimestampType(), False),
])


class ProductDemandTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("smartretail-product-demand-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def sample_orders(self):
        rows = [
            (
                "E1", datetime(2026, 1, 1, 10), "C1", "PROD-001",
                2, Decimal("10.00"), "WEB", "SAO_PAULO",
                Decimal("20.00"), date(2026, 1, 1), datetime(2026, 1, 1, 10, 5),
            ),
            (
                "E2", datetime(2026, 1, 1, 11), "C2", "PROD-001",
                3, Decimal("10.00"), "WEB", "SAO_PAULO",
                Decimal("30.00"), date(2026, 1, 1), datetime(2026, 1, 1, 11, 5),
            ),
            (
                "E3", datetime(2026, 1, 3, 10), "C1", "PROD-001",
                1, Decimal("12.00"), "MOBILE", "CAMPINAS",
                Decimal("12.00"), date(2026, 1, 3), datetime(2026, 1, 3, 10, 5),
            ),
            (
                "E4", datetime(2026, 1, 2, 10), "C3", "PROD-002",
                4, Decimal("20.00"), "PDV", "SANTOS",
                Decimal("80.00"), date(2026, 1, 2), datetime(2026, 1, 2, 10, 5),
            ),
        ]
        return self.spark.createDataFrame(rows, schema=SILVER_SCHEMA)

    def test_aggregates_product_day_metrics(self):
        result = build_product_demand_daily(self.sample_orders())
        row = (
            result
            .filter("productId = 'PROD-001' AND eventDate = DATE '2026-01-01'")
            .first()
        )

        self.assertEqual(row["unitsSold"], 5)
        self.assertEqual(row["orderCount"], 2)
        self.assertEqual(row["revenue"], Decimal("50.00"))
        self.assertEqual(row["averageUnitPrice"], Decimal("10.00"))
        self.assertEqual(row["uniqueCustomers"], 2)

    def test_zero_fills_missing_product_date_combinations(self):
        result = build_product_demand_daily(self.sample_orders())
        row = (
            result
            .filter("productId = 'PROD-002' AND eventDate = DATE '2026-01-01'")
            .first()
        )

        self.assertEqual(row["unitsSold"], 0)
        self.assertEqual(row["orderCount"], 0)
        self.assertEqual(row["revenue"], Decimal("0.00"))
        self.assertEqual(row["uniqueCustomers"], 0)
        self.assertIsNone(row["averageUnitPrice"])

    def test_output_has_one_row_per_product_date(self):
        result = build_product_demand_daily(self.sample_orders())

        self.assertEqual(result.count(), 6)
        self.assertEqual(
            result.select("productId", "eventDate").distinct().count(),
            6,
        )

    def test_generated_history_produces_continuous_365_day_calendar(self):
        from historical_data_generator import generate_silver_history

        history = generate_silver_history(
            self.spark,
            date(2025, 10, 1),
            365,
            30,
            20261002,
        )
        result = build_product_demand_daily(history)

        self.assertEqual(result.count(), 30 * 365)
        self.assertEqual(
            result.select("eventDate").distinct().count(),
            365,
        )


if __name__ == "__main__":
    unittest.main()
