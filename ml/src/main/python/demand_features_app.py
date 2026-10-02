from pyspark.sql import SparkSession

from demand_features import build_demand_features
from ml_config import (
    ML_FEATURES_PATH,
    PRODUCT_DEMAND_GOLD_PATH,
    S3_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_KEY,
)


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("smartretail-demand-features")
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
        product_demand = spark.read.parquet(
            PRODUCT_DEMAND_GOLD_PATH
        )
        features = build_demand_features(product_demand)

        if features.rdd.isEmpty():
            raise RuntimeError(
                "Demand feature dataset is empty."
            )

        (
            features
            .write
            .mode("overwrite")
            .partitionBy("eventDate")
            .parquet(ML_FEATURES_PATH)
        )

        print("Demand feature build: PASSED")
        print(f"Rows={features.count()}")
        print(
            "Products="
            f"{features.select('productId').distinct().count()}"
        )
        print(
            "Dates="
            f"{features.select('eventDate').distinct().count()}"
        )
        print(f"Input={PRODUCT_DEMAND_GOLD_PATH}")
        print(f"Output={ML_FEATURES_PATH}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
