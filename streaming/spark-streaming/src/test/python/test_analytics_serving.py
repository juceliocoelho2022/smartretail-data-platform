import os
import sys
import unittest
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


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

if MAIN_PYTHON_DIR not in sys.path:
    sys.path.insert(
        0,
        MAIN_PYTHON_DIR,
    )

from analytics_serving import (
    build_daily_read_model,
    build_summary_read_model,
)


class AnalyticsServingTest(
    unittest.TestCase
):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName(
                "smartretail-analytics-serving-test"
            )
            .getOrCreate()
        )

        cls.spark.sparkContext.setLogLevel(
            "ERROR"
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_summary_contract(
        self
    ):
        schema = StructType([
            StructField(
                "totalOrders",
                LongType(),
                False,
            ),
            StructField(
                "totalItems",
                LongType(),
                False,
            ),
            StructField(
                "totalRevenue",
                DecimalType(19, 2),
                False,
            ),
            StructField(
                "averageOrderValue",
                DecimalType(19, 2),
                False,
            ),
            StructField(
                "uniqueCustomers",
                LongType(),
                False,
            ),
            StructField(
                "uniqueProducts",
                LongType(),
                False,
            ),
            StructField(
                "goldProcessedAt",
                TimestampType(),
                False,
            ),
        ])

        source = self.spark.createDataFrame(
            [(
                5,
                14,
                Decimal("2918.60"),
                Decimal("583.72"),
                4,
                4,
                datetime(
                    2026,
                    10,
                    2,
                    17,
                    0,
                    0,
                ),
            )],
            schema,
        )

        row = (
            build_summary_read_model(
                source
            )
            .first()
        )

        self.assertEqual(
            row["id"],
            1,
        )
        self.assertEqual(
            row["total_orders"],
            5,
        )
        self.assertEqual(
            row["total_revenue"],
            Decimal("2918.60"),
        )

    def test_daily_contract(
        self
    ):
        schema = StructType([
            StructField(
                "eventDate",
                DateType(),
                False,
            ),
            StructField(
                "channel",
                StringType(),
                False,
            ),
            StructField(
                "location",
                StringType(),
                False,
            ),
            StructField(
                "totalOrders",
                LongType(),
                False,
            ),
            StructField(
                "totalItems",
                LongType(),
                False,
            ),
            StructField(
                "totalRevenue",
                DecimalType(19, 2),
                False,
            ),
            StructField(
                "averageOrderValue",
                DecimalType(19, 2),
                False,
            ),
            StructField(
                "goldProcessedAt",
                TimestampType(),
                False,
            ),
        ])

        source = self.spark.createDataFrame(
            [(
                date(2026, 10, 2),
                "WEB",
                "SAO_PAULO",
                2,
                5,
                Decimal("1000.00"),
                Decimal("500.00"),
                datetime(
                    2026,
                    10,
                    2,
                    17,
                    0,
                    0,
                ),
            )],
            schema,
        )

        row = (
            build_daily_read_model(
                source
            )
            .first()
        )

        self.assertEqual(
            row["event_date"],
            date(2026, 10, 2),
        )
        self.assertEqual(
            row["channel"],
            "WEB",
        )
        self.assertEqual(
            row["total_orders"],
            2,
        )


if __name__ == "__main__":
    unittest.main()
