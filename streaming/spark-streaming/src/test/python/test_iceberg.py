import os
import sys
import unittest
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession
from pyspark.sql.types import (
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

if MAIN_PYTHON_DIR not in sys.path:
    sys.path.insert(
        0,
        MAIN_PYTHON_DIR
    )

import iceberg_app


class IcebergAppTest(
    unittest.TestCase
):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName(
                "smartretail-iceberg-test"
            )
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel(
            "ERROR"
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_identifiers_use_configured_catalog_namespace_and_table(
        self
    ):
        self.assertEqual(
            iceberg_app.namespace_identifier(),
            "smartretail.lakehouse",
        )
        self.assertEqual(
            iceberg_app.table_identifier(),
            "smartretail.lakehouse.orders",
        )

    def test_table_exists_returns_true_when_table_is_present(
        self
    ):
        spark = MagicMock()
        row = MagicMock()
        row.tableName = "orders"

        spark.sql.return_value.collect.return_value = [
            row
        ]

        self.assertTrue(
            iceberg_app.table_exists(
                spark
            )
        )

    def test_table_exists_returns_false_when_table_is_missing(
        self
    ):
        spark = MagicMock()
        row = MagicMock()
        row.tableName = "other_table"

        spark.sql.return_value.collect.return_value = [
            row
        ]

        self.assertFalse(
            iceberg_app.table_exists(
                spark
            )
        )

    def test_align_source_to_target_adds_nullable_evolved_column(
        self
    ):
        source_schema = StructType([
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
        ])

        target_schema = StructType([
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
                "dataQualityStatus",
                StringType(),
                True,
            ),
        ])

        source = self.spark.createDataFrame(
            [
                (
                    "evt-001",
                    2,
                )
            ],
            schema=source_schema,
        )

        target = self.spark.createDataFrame(
            [],
            schema=target_schema,
        )

        result = (
            iceberg_app.align_source_to_target(
                source,
                target,
            )
            .collect()[0]
        )

        self.assertEqual(
            result.eventId,
            "evt-001",
        )
        self.assertEqual(
            result.quantity,
            2,
        )
        self.assertIsNone(
            result.dataQualityStatus
        )

    def test_time_travel_is_skipped_with_only_one_snapshot(
        self
    ):
        spark = MagicMock()

        with patch(
            "builtins.print"
        ) as print_mock:
            iceberg_app.show_time_travel_if_available(
                spark,
                [
                    {
                        "snapshot_id": 1
                    }
                ],
            )

        spark.sql.assert_not_called()
        spark.table.assert_not_called()
        print_mock.assert_called()


if __name__ == "__main__":
    unittest.main()
