import os
import sys
import unittest

from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
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

sys.path.insert(
    0,
    MAIN_PYTHON_DIR,
)

from lakehouse import build_silver_orders


class SilverOrdersTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName(
                "smartretail-silver-test"
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

        cls.bronze_schema = StructType([
            StructField(
                "payload",
                StringType(),
                False,
            ),
            StructField(
                "kafkaTopic",
                StringType(),
                False,
            ),
            StructField(
                "kafkaPartition",
                IntegerType(),
                False,
            ),
            StructField(
                "kafkaOffset",
                LongType(),
                False,
            ),
            StructField(
                "kafkaTimestamp",
                TimestampType(),
                True,
            ),
            StructField(
                "ingestedAt",
                TimestampType(),
                False,
            ),
        ])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def create_bronze_dataframe(
            self,
            payloads,
    ):
        now = datetime.now(
            timezone.utc
        ).replace(
            tzinfo=None
        )

        rows = []

        for index, payload in enumerate(
                payloads
        ):
            rows.append(
                (
                    payload,
                    "smartretail.orders.v1",
                    0,
                    index,
                    now,
                    now,
                )
            )

        return self.spark.createDataFrame(
            rows,
            self.bronze_schema,
        )

    def test_should_transform_valid_bronze_event_to_silver(
            self
    ):
        payload = """
        {
          "eventId": "SILVER-001",
          "eventType": "ORDER_CREATED",
          "eventVersion": 1,
          "occurredAt": 1790937600.0,
          "producer": "smartretail-ingestion-api",
          "correlationId": "CORR-001",
          "customerId": "CUST-SILVER-001",
          "productId": "PROD-SILVER-001",
          "quantity": 5,
          "unitPrice": 79.90,
          "channel": " mobile ",
          "location": " sao_paulo "
        }
        """

        bronze_dataframe = (
            self.create_bronze_dataframe(
                [payload]
            )
        )

        result = build_silver_orders(
            bronze_dataframe
        )

        row = result.first()

        self.assertIsNotNone(row)

        self.assertEqual(
            row.eventId,
            "SILVER-001",
        )

        self.assertEqual(
            row.customerId,
            "CUST-SILVER-001",
        )

        self.assertEqual(
            row.productId,
            "PROD-SILVER-001",
        )

        self.assertEqual(
            row.quantity,
            5,
        )

        self.assertEqual(
            row.unitPrice,
            Decimal("79.90"),
        )

        self.assertEqual(
            row.revenue,
            Decimal("399.50"),
        )

        self.assertEqual(
            row.channel,
            "MOBILE",
        )

        self.assertEqual(
            row.location,
            "SAO_PAULO",
        )

        self.assertIsNotNone(
            row.occurredAt
        )

        self.assertIsNotNone(
            row.eventDate
        )

        self.assertIsNotNone(
            row.bronzeIngestedAt
        )

        self.assertIsNotNone(
            row.silverProcessedAt
        )

    def test_should_filter_event_with_invalid_quantity(
            self
    ):
        payload = """
        {
          "eventId": "SILVER-INVALID-QUANTITY",
          "eventType": "ORDER_CREATED",
          "eventVersion": 1,
          "occurredAt": 1790937600.0,
          "producer": "smartretail-ingestion-api",
          "customerId": "CUST-INVALID",
          "productId": "PROD-INVALID",
          "quantity": 0,
          "unitPrice": 10.00,
          "channel": "WEB",
          "location": "SAO_PAULO"
        }
        """

        bronze_dataframe = (
            self.create_bronze_dataframe(
                [payload]
            )
        )

        result = build_silver_orders(
            bronze_dataframe
        )

        self.assertEqual(
            result.count(),
            0,
        )

    def test_should_filter_event_with_negative_price(
            self
    ):
        payload = """
        {
          "eventId": "SILVER-INVALID-PRICE",
          "eventType": "ORDER_CREATED",
          "eventVersion": 1,
          "occurredAt": 1790937600.0,
          "producer": "smartretail-ingestion-api",
          "customerId": "CUST-INVALID",
          "productId": "PROD-INVALID",
          "quantity": 1,
          "unitPrice": -10.00,
          "channel": "WEB",
          "location": "SAO_PAULO"
        }
        """

        bronze_dataframe = (
            self.create_bronze_dataframe(
                [payload]
            )
        )

        result = build_silver_orders(
            bronze_dataframe
        )

        self.assertEqual(
            result.count(),
            0,
        )

    def test_should_filter_malformed_json(
            self
    ):
        payload = """
        {
          "eventId": "BROKEN",
          invalid-json
        }
        """

        bronze_dataframe = (
            self.create_bronze_dataframe(
                [payload]
            )
        )

        result = build_silver_orders(
            bronze_dataframe
        )

        self.assertEqual(
            result.count(),
            0,
        )

    def test_should_remove_duplicate_event_id(
            self
    ):
        payload = """
        {
          "eventId": "SILVER-DUPLICATE-001",
          "eventType": "ORDER_CREATED",
          "eventVersion": 1,
          "occurredAt": 1790937600.0,
          "producer": "smartretail-ingestion-api",
          "customerId": "CUST-DUPLICATE",
          "productId": "PROD-DUPLICATE",
          "quantity": 2,
          "unitPrice": 50.00,
          "channel": "WEB",
          "location": "SAO_PAULO"
        }
        """

        bronze_dataframe = (
            self.create_bronze_dataframe(
                [
                    payload,
                    payload,
                ]
            )
        )

        result = build_silver_orders(
            bronze_dataframe
        )

        rows = result.collect()

        self.assertEqual(
            len(rows),
            1,
        )

        self.assertEqual(
            rows[0].eventId,
            "SILVER-DUPLICATE-001",
        )

        self.assertEqual(
            rows[0].revenue,
            Decimal("100.00"),
        )


if __name__ == "__main__":
    unittest.main()