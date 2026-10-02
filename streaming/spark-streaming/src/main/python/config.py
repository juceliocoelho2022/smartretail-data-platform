import os


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

ORDERS_TOPIC = os.getenv(
    "ORDERS_TOPIC",
    "smartretail.orders.v1"
)

SPARK_APP_NAME = os.getenv(
    "SPARK_APP_NAME",
    "smartretail-orders-streaming"
)

CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    "./checkpoints/orders"
)

BRONZE_CHECKPOINT_LOCATION = os.getenv(
    "BRONZE_CHECKPOINT_LOCATION",
    "./checkpoints/bronze-orders"
)

BRONZE_PATH = os.getenv(
    "BRONZE_PATH",
    "s3a://smartretail-bronze/orders"
)

SILVER_CHECKPOINT_LOCATION = os.getenv(
    "SILVER_CHECKPOINT_LOCATION",
    "./checkpoints/silver-orders"
)

SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "s3a://smartretail-silver/orders"
)

S3_ENDPOINT = os.getenv(
    "S3_ENDPOINT",
    "http://localhost:9000"
)

S3_ACCESS_KEY = os.getenv(
    "S3_ACCESS_KEY",
    "smartretail"
)

S3_SECRET_KEY = os.getenv(
    "S3_SECRET_KEY",
    "smartretail123"
)

S3_REGION = os.getenv(
    "S3_REGION",
    "us-east-1"
)
GOLD_DAILY_PATH = os.getenv(
    "GOLD_DAILY_PATH",
    "s3a://smartretail-gold/orders-daily"
)

GOLD_SUMMARY_PATH = os.getenv(
    "GOLD_SUMMARY_PATH",
    "s3a://smartretail-gold/orders-summary"
)

ICEBERG_CATALOG_NAME = os.getenv(
    "ICEBERG_CATALOG_NAME",
    "smartretail"
)

ICEBERG_NAMESPACE = os.getenv(
    "ICEBERG_NAMESPACE",
    "lakehouse"
)

ICEBERG_TABLE = os.getenv(
    "ICEBERG_TABLE",
    "orders"
)

ICEBERG_WAREHOUSE = os.getenv(
    "ICEBERG_WAREHOUSE",
    "s3a://smartretail-warehouse/iceberg"
)

ICEBERG_JDBC_URI = os.getenv(
    "ICEBERG_JDBC_URI",
    "jdbc:postgresql://localhost:5433/smartretail"
)

ICEBERG_JDBC_USER = os.getenv(
    "ICEBERG_JDBC_USER",
    "smartretail"
)

ICEBERG_JDBC_PASSWORD = os.getenv(
    "ICEBERG_JDBC_PASSWORD",
    "smartretail"
)


ANALYTICS_JDBC_URL = os.getenv(
    "ANALYTICS_JDBC_URL",
    "jdbc:postgresql://localhost:5433/smartretail"
)

ANALYTICS_JDBC_USER = os.getenv(
    "ANALYTICS_JDBC_USER",
    "smartretail"
)

ANALYTICS_JDBC_PASSWORD = os.getenv(
    "ANALYTICS_JDBC_PASSWORD",
    "smartretail"
)

ANALYTICS_SUMMARY_TABLE = os.getenv(
    "ANALYTICS_SUMMARY_TABLE",
    "analytics.sales_summary"
)

ANALYTICS_DAILY_TABLE = os.getenv(
    "ANALYTICS_DAILY_TABLE",
    "analytics.sales_daily"
)
