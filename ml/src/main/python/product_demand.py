from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType


MONEY_TYPE = DecimalType(19, 2)


def build_product_demand_daily(
    silver_orders: DataFrame,
) -> DataFrame:
    products = (
        silver_orders
        .select("productId")
        .distinct()
    )

    bounds = silver_orders.agg(
        F.min("eventDate").alias("minDate"),
        F.max("eventDate").alias("maxDate"),
    )

    dates = (
        bounds
        .select(
            F.explode(
                F.sequence(
                    F.col("minDate"),
                    F.col("maxDate"),
                )
            ).alias("eventDate")
        )
    )

    calendar = products.crossJoin(dates)

    daily = (
        silver_orders
        .groupBy("productId", "eventDate")
        .agg(
            F.sum("quantity")
            .cast(LongType())
            .alias("unitsSold"),
            F.countDistinct("eventId")
            .cast(LongType())
            .alias("orderCount"),
            F.round(F.sum("revenue"), 2)
            .cast(MONEY_TYPE)
            .alias("revenue"),
            F.round(
                F.sum("revenue") / F.sum("quantity"),
                2,
            )
            .cast(MONEY_TYPE)
            .alias("averageUnitPrice"),
            F.countDistinct("customerId")
            .cast(LongType())
            .alias("uniqueCustomers"),
        )
    )

    zero_money = F.lit("0.00").cast(MONEY_TYPE)
    zero_long = F.lit(0).cast(LongType())

    return (
        calendar
        .join(
            daily,
            on=["productId", "eventDate"],
            how="left",
        )
        .select(
            "eventDate",
            "productId",
            F.coalesce("unitsSold", zero_long).alias("unitsSold"),
            F.coalesce("orderCount", zero_long).alias("orderCount"),
            F.coalesce("revenue", zero_money).alias("revenue"),
            F.col("averageUnitPrice"),
            F.coalesce(
                "uniqueCustomers",
                zero_long,
            ).alias("uniqueCustomers"),
            F.current_timestamp().alias("processedAt"),
        )
        .orderBy("productId", "eventDate")
    )
