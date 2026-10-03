from ml_config import (
    ANALYTICS_PG_DSN,
    ANOMALY_STAGING_TABLE,
    ANOMALY_TARGET_TABLE,
)
from serving_publish import replace_from_staging


ANOMALY_COLUMNS = [
    "event_date",
    "product_id",
    "actual_units",
    "expected_units",
    "residual",
    "anomaly_score",
    "is_anomaly",
    "model_version",
    "detected_at",
]


def main() -> None:
    replace_from_staging(
        target_table=ANOMALY_TARGET_TABLE,
        staging_table=ANOMALY_STAGING_TABLE,
        ordered_columns=ANOMALY_COLUMNS,
        pg_dsn=ANALYTICS_PG_DSN,
    )

    print("Sales anomaly publication: PASSED")
    print(f"Target={ANOMALY_TARGET_TABLE}")


if __name__ == "__main__":
    main()
