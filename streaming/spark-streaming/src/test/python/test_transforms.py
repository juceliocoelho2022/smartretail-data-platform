import os
import sys
import unittest
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.functions import timestamp_seconds
from pyspark.sql.types import (
    BinaryType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
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

from transforms import (
    build_sales_metrics,
    parse_order_events,
)


class OrderEventTransformsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName(
                "smartretail-transforms-test"
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

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_should_parse_valid_order_event(self):
        payload = """
        {
          "eventId": "c438dc40-f400-4ed0-9eb7-28fd87cf1937",
          "eventType": "ORDER_CREATED",
          "eventVersion": 1,
          "occurredAt": 1790894174.069822798,
          "producer": "smartretail-ingestion-api",
          "correlationId": "6a8608ba-fa58-4d82-bfe0-71b08557d967",
          "customerId": "CUST-SPARK-001",
          "productId": "PROD-SPARK-001",
          "quantity": 3,
          "unitPrice": 199.90,
          "channel": "WEB",
          "location": "SAO_PAULO"
        }
        """

        schema = StructType([
            StructField(
                "value",
                BinaryType(),
                False,
            )
        ])

        kafka_dataframe = (
            self.spark.createDataFrame(
                [
                    (
                        payload.encode(
                            "utf-8"
                        ),
                    )
                ],
                schema,
            )
        )

        result = parse_order_events(
            kafka_dataframe
        )

        row = result.first()

        self.assertIsNotNone(row)

        self.assertEqual(
            row.eventId,
            "c438dc40-f400-4ed0-9eb7-28fd87cf1937",
        )

        self.assertEqual(
            row.eventType,
            "ORDER_CREATED",
        )

        self.assertEqual(
            row.eventVersion,
            1,
        )

        self.assertEqual(
            row.customerId,
            "CUST-SPARK-001",
        )

        self.assertEqual(
            row.productId,
            "PROD-SPARK-001",
        )

        self.assertEqual(
            row.quantity,
            3,
        )

        self.assertEqual(
            row.channel,
            "WEB",
        )

        self.assertEqual(
            row.location,
            "SAO_PAULO",
        )

        self.assertIsNotNone(
            row.occurredAt
        )

    def test_should_filter_invalid_timestamp(self):
        payload = """
        {
          "eventId": "INVALID-001",
          "eventType": "ORDER_CREATED",
          "eventVersion": 1,
          "occurredAt": "invalid",
          "producer": "smartretail-ingestion-api",
          "customerId": "CUST-INVALID",
          "productId": "PROD-INVALID",
          "quantity": 1,
          "unitPrice": 10.00,
          "channel": "WEB",
          "location": "SAO_PAULO"
        }
        """

        schema = StructType([
            StructField(
                "value",
                BinaryType(),
                False,
            )
        ])

        kafka_dataframe = (
            self.spark.createDataFrame(
                [
                    (
                        payload.encode(
                            "utf-8"
                        ),
                    )
                ],
                schema,
            )
        )

        result = parse_order_events(
            kafka_dataframe
        )

        self.assertEqual(
            result.count(),
            0,
        )

    def test_should_aggregate_sales_metrics(self):
        schema = StructType([
            StructField(
                "eventId",
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
                "channel",
                StringType(),
                False,
            ),
            StructField(
                "occurredAtEpoch",
                DoubleType(),
                False,
            ),
        ])

        dataframe = (
            self.spark.createDataFrame(
                [
                    (
                        "ORDER-001",
                        3,
                        Decimal("199.90"),
                        "WEB",
                        1790894174.0,
                    ),
                    (
                        "ORDER-002",
                        2,
                        Decimal("249.90"),
                        "WEB",
                        1790894184.0,
                    ),
                ],
                schema,
            )
            .withColumn(
                "occurredAt",
                timestamp_seconds(
                    "occurredAtEpoch"
                ),
            )
            .drop(
                "occurredAtEpoch"
            )
        )

        result = build_sales_metrics(
            dataframe
        )

        rows = result.collect()

        self.assertEqual(
            len(rows),
            1,
        )

        row = rows[0]

        self.assertEqual(
            row.channel,
            "WEB",
        )

        self.assertEqual(
            row.orders,
            2,
        )

        self.assertEqual(
            row.items,
            5,
        )

        self.assertEqual(
            row.revenue,
            Decimal("1099.50"),
        )


if __name__ == "__main__":
    unittest.main()