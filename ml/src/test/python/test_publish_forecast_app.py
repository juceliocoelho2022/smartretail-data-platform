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

from publish_forecast_app import FORECAST_COLUMNS, main


class PublishForecastAppTest(unittest.TestCase):

    @patch("publish_forecast_app.merge_from_staging")
    def test_main_merges_forecast_vintages_non_destructively(self, merge):
        main()

        merge.assert_called_once_with(
            target_table="analytics.demand_forecast",
            staging_table="analytics.demand_forecast_staging",
            ordered_columns=FORECAST_COLUMNS,
            conflict_columns=[
                "product_id",
                "forecast_date",
                "model_version",
                "training_cutoff_date",
            ],
            pg_dsn="dbname=smartretail user=smartretail password=smartretail host=postgres port=5432",
        )


if __name__ == "__main__":
    unittest.main()
