import os

from pyspark.sql import SparkSession

from ml_config import (
    PRODUCT_DEMAND_GOLD_PATH,
    S3_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_KEY,
)
from product_demand import build_product_demand_daily


SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "s3a://smartretail-silver/orders",
)


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("smartretail-product-demand-gold")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint.region", S3_REGION)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        silver_orders = spark.read.parquet(SILVER_PATH)
        product_demand = build_product_demand_daily(silver_orders)

        (
            product_demand
            .write
            .mode("overwrite")
            .partitionBy("eventDate")
            .parquet(PRODUCT_DEMAND_GOLD_PATH)
        )

        print("Product demand Gold build: PASSED")
        print(f"Rows={product_demand.count()}")
        print(
            "Products="
            f"{product_demand.select('productId').distinct().count()}"
        )
        print(
            "Dates="
            f"{product_demand.select('eventDate').distinct().count()}"
        )
        print(f"Input={SILVER_PATH}")
        print(f"Output={PRODUCT_DEMAND_GOLD_PATH}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
