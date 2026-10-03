import os
import sys
import unittest
from unittest.mock import patch


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_PYTHON_DIR = os.path.abspath(
    os.path.join(CURRENT_DIR, "..", "..", "main", "python")
)
if MAIN_PYTHON_DIR not in sys.path:
    sys.path.insert(0, MAIN_PYTHON_DIR)

from publish_anomaly_app import ANOMALY_COLUMNS, main


class PublishAnomalyAppTest(unittest.TestCase):

    def test_anomaly_columns_match_staging_contract(self):
        self.assertEqual(
            [
                "event_date",
                "product_id",
                "actual_units",
                "expected_units",
                "residual",
                "anomaly_score",
                "is_anomaly",
                "model_version",
                "detected_at",
            ],
            ANOMALY_COLUMNS,
        )

    @patch("publish_anomaly_app.replace_from_staging")
    def test_main_publishes_anomaly_staging_transactionally(self, replace):
        main()

        replace.assert_called_once_with(
            target_table="analytics.sales_anomaly",
            staging_table="analytics.sales_anomaly_staging",
            ordered_columns=ANOMALY_COLUMNS,
            pg_dsn="dbname=smartretail user=smartretail password=smartretail host=postgres port=5432",
        )


if __name__ == "__main__":
    unittest.main()
