import random
import uuid

from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


MONEY = Decimal("0.01")
CHANNELS = ("WEB", "MOBILE", "PDV")
LOCATIONS = (
    "SAO_PAULO",
    "CAMPINAS",
    "SANTOS",
    "SOROCABA",
    "RIBEIRAO_PRETO",
)
WEEKDAY_FACTORS = (
    0.82,
    0.88,
    1.00,
    1.06,
    1.18,
    1.32,
    1.22,
)

SILVER_HISTORY_SCHEMA = StructType([
    StructField("eventId", StringType(), False),
    StructField("occurredAt", TimestampType(), False),
    StructField("customerId", StringType(), False),
    StructField("productId", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unitPrice", DecimalType(19, 2), False),
    StructField("channel", StringType(), False),
    StructField("location", StringType(), False),
    StructField("revenue", DecimalType(19, 2), False),
    StructField("eventDate", DateType(), False),
    StructField("silverProcessedAt", TimestampType(), False),
])


def _split_units(units: int, order_count: int) -> list[int]:
    base, remainder = divmod(units, order_count)
    return [
        base + (1 if index < remainder else 0)
        for index in range(order_count)
    ]


def _daily_units(
    rng: random.Random,
    *,
    product_index: int,
    day_offset: int,
    current_date: date,
    days: int,
) -> int:
    base_demand = 9 + ((product_index * 7) % 11)
    weekday_factor = WEEKDAY_FACTORS[current_date.weekday()]
    trend_factor = 1.0 + (day_offset * (0.00125 + product_index * 0.00001))
    noise_factor = 1.0 + rng.uniform(-0.08, 0.08)

    units = base_demand * weekday_factor * trend_factor * noise_factor

    spike_offsets = {
        days - 22,
        days - 15,
        days - 8,
        days - 1,
    }
    spike_products = {1, 7, 13, 19, 25}

    if product_index in spike_products and day_offset in spike_offsets:
        units *= 4.0

    return max(4, int(round(units)))


def generate_silver_history(
    spark: SparkSession,
    start_date: date,
    days: int,
    product_count: int,
    seed: int,
) -> DataFrame:
    if days <= 0:
        raise ValueError("days must be greater than zero")
    if product_count <= 0:
        raise ValueError("product_count must be greater than zero")

    rng = random.Random(seed)
    rows = []

    for day_offset in range(days):
        current_date = start_date + timedelta(days=day_offset)

        for product_index in range(1, product_count + 1):
            product_id = f"PROD-{product_index:03d}"
            units = _daily_units(
                rng,
                product_index=product_index,
                day_offset=day_offset,
                current_date=current_date,
                days=days,
            )

            order_count = 2 + ((product_index + day_offset) % 2)
            quantities = _split_units(units, order_count)

            unit_price = (
                Decimal("14.90")
                + Decimal(product_index) * Decimal("3.25")
            ).quantize(MONEY, rounding=ROUND_HALF_UP)

            for order_index, quantity in enumerate(quantities):
                occurred_at = datetime.combine(
                    current_date,
                    time(
                        hour=9 + order_index * 3,
                        minute=(product_index * 7) % 60,
                    ),
                )

                event_key = (
                    f"smartretail-ml:{seed}:{current_date.isoformat()}:"
                    f"{product_id}:{order_index}"
                )
                event_id = str(
                    uuid.uuid5(uuid.NAMESPACE_URL, event_key)
                )

                customer_number = (
                    (
                        product_index * 37
                        + day_offset * 11
                        + order_index * 17
                    )
                    % 500
                ) + 1

                channel = CHANNELS[
                    (product_index + day_offset + order_index)
                    % len(CHANNELS)
                ]
                location = LOCATIONS[
                    (product_index * 3 + day_offset + order_index)
                    % len(LOCATIONS)
                ]

                revenue = (
                    unit_price * Decimal(quantity)
                ).quantize(MONEY, rounding=ROUND_HALF_UP)

                rows.append((
                    event_id,
                    occurred_at,
                    f"CUST-{customer_number:04d}",
                    product_id,
                    quantity,
                    unit_price,
                    channel,
                    location,
                    revenue,
                    current_date,
                    occurred_at + timedelta(minutes=5),
                ))

    return spark.createDataFrame(
        rows,
        schema=SILVER_HISTORY_SCHEMA,
    )
