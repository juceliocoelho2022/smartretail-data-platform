from config import (
    GOLD_SUMMARY_PATH,
    SILVER_PATH,
)
from iceberg_app import (
    create_spark_session,
    table_identifier,
)


def validate_pipeline_counts(
    silver_count: int,
    iceberg_count: int,
    gold_total_orders: int,
) -> None:
    if silver_count != iceberg_count:
        raise RuntimeError(
            "Post-load validation failed: "
            f"Silver={silver_count}, "
            f"Iceberg={iceberg_count}"
        )

    if silver_count != gold_total_orders:
        raise RuntimeError(
            "Post-load validation failed: "
            f"Silver={silver_count}, "
            f"Gold totalOrders={gold_total_orders}"
        )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    silver_count = (
        spark.read
        .format("parquet")
        .load(
            SILVER_PATH
        )
        .count()
    )

    iceberg_count = (
        spark.table(
            table_identifier()
        )
        .count()
    )

    gold_summary = (
        spark.read
        .format("parquet")
        .load(
            GOLD_SUMMARY_PATH
        )
    )

    if gold_summary.isEmpty():
        raise RuntimeError(
            "Post-load validation failed: "
            "Gold summary is empty."
        )

    gold_total_orders = int(
        gold_summary
        .select(
            "totalOrders"
        )
        .first()["totalOrders"]
    )

    validate_pipeline_counts(
        silver_count,
        iceberg_count,
        gold_total_orders,
    )

    print(
        "Post-load validation: PASSED"
    )
    print(
        f"Silver rows={silver_count}"
    )
    print(
        f"Iceberg rows={iceberg_count}"
    )
    print(
        f"Gold totalOrders={gold_total_orders}"
    )

    spark.stop()


if __name__ == "__main__":
    main()
