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