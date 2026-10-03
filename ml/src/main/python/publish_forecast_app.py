from ml_config import (
    ANALYTICS_PG_DSN,
    FORECAST_STAGING_TABLE,
    FORECAST_TARGET_TABLE,
)
from serving_publish import replace_from_staging


FORECAST_COLUMNS = [
    "product_id",
    "forecast_date",
    "predicted_units",
    "model_name",
    "model_version",
    "training_cutoff_date",
    "generated_at",
]


def main() -> None:
    replace_from_staging(
        target_table=FORECAST_TARGET_TABLE,
        staging_table=FORECAST_STAGING_TABLE,
        ordered_columns=FORECAST_COLUMNS,
        pg_dsn=ANALYTICS_PG_DSN,
    )

    print("Demand forecast publication: PASSED")
    print(f"Target={FORECAST_TARGET_TABLE}")


if __name__ == "__main__":
    main()
