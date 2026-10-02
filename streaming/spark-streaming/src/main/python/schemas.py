from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DecimalType,
)


ORDER_EVENT_SCHEMA = StructType([
    StructField("eventId", StringType(), False),
    StructField("eventType", StringType(), False),
    StructField("eventVersion", IntegerType(), False),
    StructField("occurredAt", DoubleType(), False),
    StructField("producer", StringType(), False),
    StructField("correlationId", StringType(), True),
    StructField("customerId", StringType(), False),
    StructField("productId", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unitPrice", DecimalType(19, 2), False),
    StructField("channel", StringType(), False),
    StructField("location", StringType(), False),
])