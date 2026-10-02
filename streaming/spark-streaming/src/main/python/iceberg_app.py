from pyspark.sql import SparkSession

from config import (
    ICEBERG_CATALOG_NAME,
    ICEBERG_JDBC_PASSWORD,
    ICEBERG_JDBC_URI,
    ICEBERG_JDBC_USER,
    ICEBERG_NAMESPACE,
    ICEBERG_TABLE,
    ICEBERG_WAREHOUSE,
    S3_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_KEY,
    SILVER_PATH,
)


ICEBERG_APP_NAME = "smartretail-iceberg"
SOURCE_VIEW = "silver_orders_source"


def create_spark_session() -> SparkSession:
    catalog_prefix = (
        f"spark.sql.catalog."
        f"{ICEBERG_CATALOG_NAME}"
    )

    return (
        SparkSession.builder
        .appName(
            ICEBERG_APP_NAME
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC"
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions."
            "IcebergSparkSessionExtensions"
        )
        .config(
            catalog_prefix,
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            f"{catalog_prefix}.type",
            "jdbc"
        )
        .config(
            f"{catalog_prefix}.uri",
            ICEBERG_JDBC_URI
        )
        .config(
            f"{catalog_prefix}.jdbc.user",
            ICEBERG_JDBC_USER
        )
        .config(
            f"{catalog_prefix}.jdbc.password",
            ICEBERG_JDBC_PASSWORD
        )
        .config(
            f"{catalog_prefix}.warehouse",
            ICEBERG_WAREHOUSE
        )
        .config(
            f"{catalog_prefix}.io-impl",
            "org.apache.iceberg.hadoop.HadoopFileIO"
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
            "org.apache.hadoop.fs.s3a."
            "SimpleAWSCredentialsProvider"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            S3_REGION
        )
        .getOrCreate()
    )


def table_identifier() -> str:
    return (
        f"{ICEBERG_CATALOG_NAME}."
        f"{ICEBERG_NAMESPACE}."
        f"{ICEBERG_TABLE}"
    )


def namespace_identifier() -> str:
    return (
        f"{ICEBERG_CATALOG_NAME}."
        f"{ICEBERG_NAMESPACE}"
    )


def table_exists(
    spark: SparkSession
) -> bool:
    rows = spark.sql(
        f"SHOW TABLES IN "
        f"{namespace_identifier()}"
    ).collect()

    return any(
        row.tableName == ICEBERG_TABLE
        for row in rows
    )


def show_snapshots(
    spark: SparkSession
):
    table = table_identifier()

    snapshots = spark.sql(
        f"""
        SELECT
            committed_at,
            snapshot_id,
            parent_id,
            operation
        FROM {table}.snapshots
        ORDER BY committed_at
        """
    )

    print("Iceberg snapshots:")
    snapshots.show(
        truncate=False
    )

    return snapshots.collect()


def show_time_travel_if_available(
    spark: SparkSession,
    snapshots
):
    if len(snapshots) < 2:
        print(
            "Time travel requires at least "
            "two snapshots. Run the job again "
            "after new Silver data is available."
        )
        return

    previous_snapshot_id = (
        snapshots[-2]["snapshot_id"]
    )

    table = table_identifier()

    previous_count = spark.sql(
        f"""
        SELECT COUNT(*) AS total
        FROM {table}
        VERSION AS OF {previous_snapshot_id}
        """
    ).first()["total"]

    current_count = spark.table(
        table
    ).count()

    print(
        "Time travel validation:"
    )
    print(
        f"Previous snapshot "
        f"{previous_snapshot_id}: "
        f"{previous_count} rows"
    )
    print(
        f"Current table: "
        f"{current_count} rows"
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print(
        f"Reading Silver dataset: "
        f"{SILVER_PATH}"
    )

    silver_orders = (
        spark.read
        .format("parquet")
        .load(
            SILVER_PATH
        )
    )

    if silver_orders.isEmpty():
        raise RuntimeError(
            "Silver dataset is empty. "
            "Iceberg table cannot be generated."
        )

    print(
        f"Silver records: "
        f"{silver_orders.count()}"
    )

    silver_orders.createOrReplaceTempView(
        SOURCE_VIEW
    )

    namespace = namespace_identifier()
    table = table_identifier()

    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS "
        f"{namespace}"
    )

    if table_exists(spark):
        print(
            f"Refreshing Iceberg table: "
            f"{table}"
        )

        spark.sql(
            f"""
            INSERT OVERWRITE {table}
            SELECT *
            FROM {SOURCE_VIEW}
            """
        )
    else:
        print(
            f"Creating Iceberg table: "
            f"{table}"
        )

        spark.sql(
            f"""
            CREATE TABLE {table}
            USING iceberg
            PARTITIONED BY (
                days(occurredAt)
            )
            TBLPROPERTIES (
                'format-version' = '2'
            )
            AS
            SELECT *
            FROM {SOURCE_VIEW}
            """
        )

    current_count = spark.table(
        table
    ).count()

    print(
        f"Iceberg table rows: "
        f"{current_count}"
    )

    spark.table(
        table
    ).show(
        truncate=False
    )

    snapshots = show_snapshots(
        spark
    )

    show_time_travel_if_available(
        spark,
        snapshots
    )

    print(
        f"Iceberg warehouse: "
        f"{ICEBERG_WAREHOUSE}"
    )

    spark.stop()


if __name__ == "__main__":
    main()
