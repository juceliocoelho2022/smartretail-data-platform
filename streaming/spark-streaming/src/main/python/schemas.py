from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    DecimalType,
    TimestampType,
    DateType,
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


BRONZE_EVENT_SCHEMA = StructType([
    StructField("kafkaKey", StringType(), True),
    StructField("payload", StringType(), False),
    StructField("kafkaTopic", StringType(), False),
    StructField("kafkaPartition", IntegerType(), False),
    StructField("kafkaOffset", LongType(), False),
    StructField("kafkaTimestamp", TimestampType(), True),
    StructField("ingestedAt", TimestampType(), False),
    StructField("ingestionDate", DateType(), True),
])