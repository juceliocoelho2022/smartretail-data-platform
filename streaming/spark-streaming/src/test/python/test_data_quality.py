import os
import sys
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
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

if MAIN_PYTHON_DIR not in sys.path:
    sys.path.insert(
        0,
        MAIN_PYTHON_DIR
    )

from data_quality import (
    assert_silver_quality,
    collect_silver_quality_metrics,
    find_quality_failures,
)


SILVER_QUALITY_SCHEMA = StructType([
    StructField(
        "eventId",
        StringType(),
        True,
    ),
    StructField(
        "customerId",
        StringType(),
        True,
    ),
    StructField(
        "productId",
        StringType(),
        True,
    ),
    StructField(
        "quantity",
        IntegerType(),
        True,
    ),
    StructField(
        "unitPrice",
        DoubleType(),
        True,
    ),
])


class DataQualityTest(
    unittest.TestCase
):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName(
                "smartretail-data-quality-test"
            )
            .getOrCreate()
        )

        cls.spark.sparkContext.setLogLevel(
            "ERROR"
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def create_df(
        self,
        rows,
    ):
        return self.spark.createDataFrame(
            rows,
            schema=SILVER_QUALITY_SCHEMA,
        )

    def test_clean_dataset_passes_quality_gate(
        self
    ):
        dataframe = self.create_df([
            (
                "evt-001",
                "cust-001",
                "prod-001",
                2,
                10.0,
            ),
            (
                "evt-002",
                "cust-002",
                "prod-002",
                1,
                20.0,
            ),
        ])

        metrics = assert_silver_quality(
            dataframe
        )

        self.assertEqual(
            metrics["totalRows"],
            2,
        )

        self.assertEqual(
            find_quality_failures(
                metrics
            ),
            {},
        )

    def test_invalid_business_values_are_reported(
        self
    ):
        dataframe = self.create_df([
            (
                "evt-001",
                "cust-001",
                "prod-001",
                0,
                -1.0,
            ),
            (
                None,
                None,
                None,
                1,
                10.0,
            ),
        ])

        metrics = (
            collect_silver_quality_metrics(
                dataframe
            )
        )

        failures = find_quality_failures(
            metrics
        )

        self.assertEqual(
            failures["nullEventId"],
            1,
        )
        self.assertEqual(
            failures["nullCustomerId"],
            1,
        )
        self.assertEqual(
            failures["nullProductId"],
            1,
        )
        self.assertEqual(
            failures["invalidQuantity"],
            1,
        )
        self.assertEqual(
            failures["invalidUnitPrice"],
            1,
        )

    def test_duplicate_event_id_fails_quality_gate(
        self
    ):
        dataframe = self.create_df([
            (
                "evt-001",
                "cust-001",
                "prod-001",
                1,
                10.0,
            ),
            (
                "evt-001",
                "cust-001",
                "prod-001",
                1,
                10.0,
            ),
        ])

        metrics = (
            collect_silver_quality_metrics(
                dataframe
            )
        )

        self.assertEqual(
            metrics["duplicateRows"],
            1,
        )

        with self.assertRaises(
            RuntimeError
        ):
            assert_silver_quality(
                dataframe
            )


if __name__ == "__main__":
    unittest.main()
