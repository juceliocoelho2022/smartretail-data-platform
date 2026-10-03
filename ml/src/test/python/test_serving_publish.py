import os
import sys
import unittest

from unittest.mock import MagicMock, Mock, patch


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_PYTHON_DIR = os.path.abspath(
    os.path.join(CURRENT_DIR, "..", "..", "main", "python")
)
sys.path.insert(0, MAIN_PYTHON_DIR)

from serving_publish import replace_from_staging, stage_dataframe


class _Writer:
    def __init__(self, events, fail=False):
        self.events = events
        self.fail = fail

    def format(self, value):
        self.format_name = value
        return self

    def options(self, **kwargs):
        self.options_value = kwargs
        return self

    def mode(self, value):
        self.mode_name = value
        return self

    def save(self):
        self.events.append("append")
        if self.fail:
            raise RuntimeError("staging append failed")


class _DataFrame:
    def __init__(self, events, fail=False):
        self.write = _Writer(events, fail=fail)


class ServingPublishTest(unittest.TestCase):

    @patch("serving_publish._truncate_staging_table")
    def test_stage_dataframe_truncates_before_spark_append(self, truncate):
        events = []
        truncate.side_effect = lambda *args, **kwargs: events.append("truncate")
        jdbc_options = {
            "url": "jdbc:postgresql://postgres:5432/smartretail",
            "user": "smartretail",
            "password": "smartretail",
            "driver": "org.postgresql.Driver",
            "pg_dsn": "dbname=smartretail user=smartretail host=postgres",
        }

        stage_dataframe(
            _DataFrame(events),
            "analytics.demand_forecast_staging",
            jdbc_options,
        )

        self.assertEqual(events, ["truncate", "append"])

    @patch("serving_publish._truncate_staging_table")
    def test_stage_dataframe_propagates_append_failure(self, truncate):
        events = []
        truncate.side_effect = lambda *args, **kwargs: events.append("truncate")

        with self.assertRaisesRegex(RuntimeError, "staging append failed"):
            stage_dataframe(
                _DataFrame(events, fail=True),
                "analytics.demand_forecast_staging",
                {
                    "url": "jdbc:postgresql://postgres:5432/smartretail",
                    "user": "smartretail",
                    "password": "smartretail",
                    "driver": "org.postgresql.Driver",
                    "pg_dsn": "dbname=smartretail user=smartretail host=postgres",
                },
            )

        self.assertEqual(events, ["truncate", "append"])

    @patch("serving_publish.psycopg2.connect")
    def test_replace_from_staging_commits_single_transaction(self, connect):
        connection = MagicMock()
        cursor = Mock()
        connection.cursor.return_value.__enter__.return_value = cursor
        connect.return_value = connection

        replace_from_staging(
            target_table="analytics.demand_forecast",
            staging_table="analytics.demand_forecast_staging",
            ordered_columns=[
                "product_id",
                "forecast_date",
                "predicted_units",
                "model_name",
                "model_version",
                "training_cutoff_date",
                "generated_at",
            ],
            pg_dsn="dbname=smartretail user=smartretail host=postgres",
        )

        self.assertEqual(cursor.execute.call_count, 3)
        connection.commit.assert_called_once_with()
        connection.rollback.assert_not_called()
        connection.close.assert_called_once_with()

    @patch("serving_publish.psycopg2.connect")
    def test_replace_from_staging_rolls_back_on_sql_failure(self, connect):
        connection = MagicMock()
        cursor = Mock()
        connection.cursor.return_value.__enter__.return_value = cursor
        cursor.execute.side_effect = [None, RuntimeError("insert failed")]
        connect.return_value = connection

        with self.assertRaisesRegex(RuntimeError, "insert failed"):
            replace_from_staging(
                target_table="analytics.demand_forecast",
                staging_table="analytics.demand_forecast_staging",
                ordered_columns=["product_id", "forecast_date"],
                pg_dsn="dbname=smartretail user=smartretail host=postgres",
            )

        connection.commit.assert_not_called()
        connection.rollback.assert_called_once_with()
        connection.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
