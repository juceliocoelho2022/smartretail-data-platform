import os
import sys
import unittest

from datetime import (
    date,
    datetime,
)
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

sys.path.insert(
    0,
    MAIN_PYTHON_DIR,
)

from lakehouse import (
    build_gold_daily_sales,
    build_gold_summary,
)


class GoldOrdersTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName(
                "smartretail-gold-test"
            )
            .config(
                "spark.ui.enabled",
                "false",
            )
            .config(
                "spark.sql.session.timeZone",
                "UTC",
            )
            .getOrCreate()
        )

        cls.spark.sparkContext.setLogLevel(
            "ERROR"
        )

        schema = StructType([
            StructField(
                "eventId",
                StringType(),
                False,
            ),
            StructField(
                "customerId",
                StringType(),
                False,
            ),
            StructField(
                "productId",
                StringType(),
                False,
            ),
            StructField(
                "quantity",
                IntegerType(),
                False,
            ),
            StructField(
                "unitPrice",
                DecimalType(19, 2),
                False,
            ),
            StructField(
                "revenue",
                DecimalType(19, 2),
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
                "eventDate",
                DateType(),
                False,
            ),
            StructField(
                "occurredAt",
                TimestampType(),
                False,
            ),
        ])

        event_date = date(
            2026,
            10,
            2,
        )

        occurred_at = datetime(
            2026,
            10,
            2,
            10,
            0,
            0,
        )

        cls.silver_orders = (
            cls.spark.createDataFrame(
                [
                    (
                        "ORDER-001",
                        "CUST-001",
                        "PROD-001",
                        2,
                        Decimal("100.00"),
                        Decimal("200.00"),
                        "WEB",
                        "SAO_PAULO",
                        event_date,
                        occurred_at,
                    ),
                    (
                        "ORDER-002",
                        "CUST-002",
                        "PROD-002",
                        1,
                        Decimal("50.00"),
                        Decimal("50.00"),
                        "WEB",
                        "SAO_PAULO",
                        event_date,
                        occurred_at,
                    ),
                    (
                        "ORDER-003",
                        "CUST-001",
                        "PROD-003",
                        3,
                        Decimal("100.00"),
                        Decimal("300.00"),
                        "MOBILE",
                        "RIO_DE_JANEIRO",
                        event_date,
                        occurred_at,
                    ),
                ],
                schema,
            )
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_should_build_daily_sales_kpis(
            self
    ):

        result = (
            build_gold_daily_sales(
                self.silver_orders
            )
        )

        rows = {
            (
                row.channel,
                row.location,
            ): row
            for row in result.collect()
        }

        self.assertEqual(
            len(rows),
            2,
        )

        web = rows[
            (
                "WEB",
                "SAO_PAULO",
            )
        ]

        self.assertEqual(
            web.totalOrders,
            2,
        )

        self.assertEqual(
            web.totalItems,
            3,
        )

        self.assertEqual(
            web.totalRevenue,
            Decimal("250.00"),
        )

        self.assertEqual(
            web.averageOrderValue,
            Decimal("125.00"),
        )

        mobile = rows[
            (
                "MOBILE",
                "RIO_DE_JANEIRO",
            )
        ]

        self.assertEqual(
            mobile.totalOrders,
            1,
        )

        self.assertEqual(
            mobile.totalItems,
            3,
        )

        self.assertEqual(
            mobile.totalRevenue,
            Decimal("300.00"),
        )

        self.assertEqual(
            mobile.averageOrderValue,
            Decimal("300.00"),
        )

    def test_should_build_global_summary(
            self
    ):

        result = (
            build_gold_summary(
                self.silver_orders
            )
        )

        row = result.first()

        self.assertEqual(
            row.totalOrders,
            3,
        )

        self.assertEqual(
            row.totalItems,
            6,
        )

        self.assertEqual(
            row.totalRevenue,
            Decimal("550.00"),
        )

        self.assertEqual(
            row.averageOrderValue,
            Decimal("183.33"),
        )

        self.assertEqual(
            row.uniqueCustomers,
            2,
        )

        self.assertEqual(
            row.uniqueProducts,
            3,
        )


if __name__ == "__main__":
    unittest.main()