from pyspark.sql import SparkSession

from analytics_serving import (
    build_daily_read_model,
    build_summary_read_model,
)
from config import (
    ANALYTICS_DAILY_TABLE,
    ANALYTICS_JDBC_PASSWORD,
    ANALYTICS_JDBC_URL,
    ANALYTICS_JDBC_USER,
    ANALYTICS_SUMMARY_TABLE,
    GOLD_DAILY_PATH,
    GOLD_SUMMARY_PATH,
    S3_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_KEY,
)


APP_NAME = "smartretail-analytics-export"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(APP_NAME)
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            S3_ENDPOINT,
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            S3_ACCESS_KEY,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            S3_SECRET_KEY,
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            (
                "org.apache.hadoop.fs.s3a."
                "SimpleAWSCredentialsProvider"
            ),
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            S3_REGION,
        )
        .getOrCreate()
    )


def truncate_serving_tables(
    spark: SparkSession,
) -> None:
    jvm = spark.sparkContext._gateway.jvm

    context_loader = (
        jvm.java.lang.Thread
        .currentThread()
        .getContextClassLoader()
    )

    driver_class = context_loader.loadClass(
        "org.postgresql.Driver"
    )

    driver = (
        driver_class
        .getDeclaredConstructor()
        .newInstance()
    )

    properties = jvm.java.util.Properties()
    properties.setProperty(
        "user",
        ANALYTICS_JDBC_USER,
    )
    properties.setProperty(
        "password",
        ANALYTICS_JDBC_PASSWORD,
    )

    connection = driver.connect(
        ANALYTICS_JDBC_URL,
        properties,
    )

    if connection is None:
        raise RuntimeError(
            "PostgreSQL JDBC driver did not "
            "accept the analytics URL."
        )

    statement = None

    try:
        connection.setAutoCommit(False)
        statement = connection.createStatement()

        statement.executeUpdate(
            "TRUNCATE TABLE "
            f"{ANALYTICS_DAILY_TABLE}, "
            f"{ANALYTICS_SUMMARY_TABLE}"
        )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        if statement is not None:
            statement.close()

        connection.close()


def jdbc_options() -> dict:
    return {
        "user": ANALYTICS_JDBC_USER,
        "password": ANALYTICS_JDBC_PASSWORD,
        "driver": "org.postgresql.Driver",
    }


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    gold_summary = (
        spark.read
        .format("parquet")
        .load(GOLD_SUMMARY_PATH)
    )

    gold_daily = (
        spark.read
        .format("parquet")
        .load(GOLD_DAILY_PATH)
    )

    if gold_summary.isEmpty():
        raise RuntimeError(
            "Analytics export failed: "
            "Gold summary is empty."
        )

    if gold_daily.isEmpty():
        raise RuntimeError(
            "Analytics export failed: "
            "Gold daily dataset is empty."
        )

    summary_read_model = (
        build_summary_read_model(
            gold_summary
        )
    )

    daily_read_model = (
        build_daily_read_model(
            gold_daily
        )
    )

    truncate_serving_tables(spark)

    summary_read_model.write.jdbc(
        url=ANALYTICS_JDBC_URL,
        table=ANALYTICS_SUMMARY_TABLE,
        mode="append",
        properties=jdbc_options(),
    )

    daily_read_model.write.jdbc(
        url=ANALYTICS_JDBC_URL,
        table=ANALYTICS_DAILY_TABLE,
        mode="append",
        properties=jdbc_options(),
    )

    summary_count = summary_read_model.count()
    daily_count = daily_read_model.count()

    if summary_count != 1:
        raise RuntimeError(
            "Analytics export failed: "
            f"summary rows={summary_count}"
        )

    if daily_count <= 0:
        raise RuntimeError(
            "Analytics export failed: "
            f"daily rows={daily_count}"
        )

    print("Analytics serving export: PASSED")
    print(
        f"Summary rows={summary_count}"
    )
    print(
        f"Daily rows={daily_count}"
    )

    spark.stop()


if __name__ == "__main__":
    main()
