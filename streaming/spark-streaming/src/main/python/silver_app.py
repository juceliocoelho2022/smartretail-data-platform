import time

from pyspark.sql import SparkSession

from config import (
    BRONZE_PATH,
    S3_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_KEY,
    SILVER_CHECKPOINT_LOCATION,
    SILVER_PATH,
)

from lakehouse import (
    build_silver_orders,
)

from schemas import (
    BRONZE_EVENT_SCHEMA,
)


SILVER_APP_NAME = (
    "smartretail-silver-orders"
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(
            SILVER_APP_NAME
        )

        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )

        .config(
            "spark.hadoop.fs.s3a.endpoint",
            S3_ENDPOINT
        )

        .config(
            "spark.hadoop.fs.s3a.access.key",
            S3_ACCESS_KEY
        )

        .config(
            "spark.hadoop.fs.s3a.secret.key",
            S3_SECRET_KEY
        )

        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )

        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false"
        )

        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )

        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            S3_REGION
        )

        .getOrCreate()
    )


def path_exists(
        spark: SparkSession,
        path: str
) -> bool:

    hadoop_path = (
        spark
        ._jvm
        .org.apache.hadoop.fs.Path(
            path
        )
    )

    filesystem = (
        hadoop_path.getFileSystem(
            spark
            ._jsc
            .hadoopConfiguration()
        )
    )

    return filesystem.exists(
        hadoop_path
    )


def wait_for_bronze(
        spark: SparkSession,
        timeout_seconds: int = 180
):

    start = time.time()

    while (
            time.time() - start
            < timeout_seconds
    ):

        if path_exists(
                spark,
                BRONZE_PATH
        ):
            print(
                f"Bronze disponível: "
                f"{BRONZE_PATH}"
            )
            return

        print(
            "Aguardando Bronze..."
        )

        time.sleep(5)

    raise TimeoutError(
        f"Bronze não encontrada: "
        f"{BRONZE_PATH}"
    )


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    wait_for_bronze(
        spark
    )

    bronze_stream = (
        spark.readStream
        .format("parquet")
        .schema(
            BRONZE_EVENT_SCHEMA
        )
        .option(
            "maxFilesPerTrigger",
            10
        )
        .load(
            BRONZE_PATH
        )
    )

    silver_orders = (
        build_silver_orders(
            bronze_stream
        )
    )

    query = (
        silver_orders
        .writeStream
        .format("parquet")
        .outputMode("append")

        .option(
            "path",
            SILVER_PATH
        )

        .option(
            "checkpointLocation",
            SILVER_CHECKPOINT_LOCATION
        )

        .partitionBy(
            "eventDate"
        )

        .queryName(
            "smartretail-silver-orders"
        )

        .start()
    )

    print(
        f"Silver query started: "
        f"{query.name}"
    )

    print(
        f"Bronze source: "
        f"{BRONZE_PATH}"
    )

    print(
        f"Silver destination: "
        f"{SILVER_PATH}"
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()