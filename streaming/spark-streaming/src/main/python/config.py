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
