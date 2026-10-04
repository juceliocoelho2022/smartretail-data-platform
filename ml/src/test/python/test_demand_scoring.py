import math
import os
import sys
import unittest

from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_PYTHON_DIR = os.path.abspath(
    os.path.join(CURRENT_DIR, "..", "..", "main", "python")
)
sys.path.insert(0, MAIN_PYTHON_DIR)

from demand_scoring import (
    generate_recursive_forecast,
    score_and_stage_forecast,
)


class _PredictionModel:
    def __init__(self, expression):
        self.expression = expression

    def transform(self, dataframe):
        return dataframe.withColumn(
            "prediction",
            self.expression(dataframe),
        )


class DemandScoringTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("demand-scoring-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _history(self):
        products = self.spark.sql(
            """
            SELECT *
            FROM VALUES
                ('P001', CAST(10.0 AS DOUBLE)),
                ('P002', CAST(20.0 AS DOUBLE))
            AS products(productId, baseUnits)
            """
        )

        offsets = self.spark.range(35).select(
            F.col("id").cast("int").alias("offset")
        )

        return (
            products
            .crossJoin(offsets)
            .select(
                "productId",
                F.date_add(
                    F.lit("2026-01-01").cast("date"),
                    F.col("offset"),
                ).alias("eventDate"),
                (
                    F.col("baseUnits")
                    + F.pmod(F.col("offset"), F.lit(7)).cast("double")
                ).alias("unitsSold"),
            )
        )

    def test_recursive_forecast_clamps_negative_predictions_and_builds_7_days(self):
        model = _PredictionModel(
            lambda df: -F.abs(F.col("lag1"))
        )

        forecast = generate_recursive_forecast(
            model,
            self._history(),
            horizon_days=7,
        )

        rows = forecast.orderBy("productId", "forecastDate").collect()
        self.assertEqual(len(rows), 14)
        self.assertTrue(all(row["predictedUnits"] == 0.0 for row in rows))

        grouped = {}
        for row in rows:
            grouped.setdefault(row["productId"], []).append(row["forecastDate"])

        self.assertEqual(set(grouped), {"P001", "P002"})
        for dates in grouped.values():
            self.assertEqual(len(dates), 7)
            self.assertEqual(
                dates,
                [date(2026, 2, 5) + timedelta(days=i) for i in range(7)],
            )

    def test_non_finite_prediction_fails_before_staging(self):
        stage = Mock()
        model = _PredictionModel(
            lambda df: F.lit(float("nan"))
        )

        with patch("demand_scoring.get_champion_version") as champion_lookup:
            champion_lookup.return_value = SimpleNamespace(version="1")
            with self.assertRaisesRegex(RuntimeError, "non-finite"):
                score_and_stage_forecast(
                    history=self._history(),
                    client=Mock(),
                    jdbc_options={"url": "jdbc:test", "pg_dsn": "test"},
                    stage_fn=stage,
                    model_loader=lambda uri: model,
                    horizon_days=7,
                )

        stage.assert_not_called()

    def test_missing_champion_fails_before_model_load_or_staging(self):
        stage = Mock()
        loader = Mock()

        with patch("demand_scoring.get_champion_version") as champion_lookup:
            champion_lookup.return_value = None
            with self.assertRaisesRegex(RuntimeError, "champion"):
                score_and_stage_forecast(
                    history=self._history(),
                    client=Mock(),
                    jdbc_options={"url": "jdbc:test", "pg_dsn": "test"},
                    stage_fn=stage,
                    model_loader=loader,
                    horizon_days=7,
                )

        loader.assert_not_called()
        stage.assert_not_called()


if __name__ == "__main__":
    unittest.main()
