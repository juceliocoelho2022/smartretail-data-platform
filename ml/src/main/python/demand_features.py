from datetime import timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_demand_features(
    daily: DataFrame,
) -> DataFrame:
    history_window = (
        Window
        .partitionBy("productId")
        .orderBy("eventDate")
    )

    rolling_7 = history_window.rowsBetween(-7, -1)
    rolling_28 = history_window.rowsBetween(-28, -1)

    features = (
        daily
        .withColumn(
            "lag1",
            F.lag("unitsSold", 1).over(history_window),
        )
        .withColumn(
            "lag7",
            F.lag("unitsSold", 7).over(history_window),
        )
        .withColumn(
            "lag14",
            F.lag("unitsSold", 14).over(history_window),
        )
        .withColumn(
            "rollingMean7",
            F.avg("unitsSold").over(rolling_7),
        )
        .withColumn(
            "rollingMean28",
            F.avg("unitsSold").over(rolling_28),
        )
        .withColumn(
            "rollingStd7",
            F.stddev_samp("unitsSold").over(rolling_7),
        )
        .withColumn(
            "historyCount28",
            F.count("unitsSold").over(rolling_28),
        )
        .withColumn(
            "dayOfWeek",
            F.dayofweek("eventDate"),
        )
        .withColumn(
            "dayOfMonth",
            F.dayofmonth("eventDate"),
        )
        .withColumn(
            "month",
            F.month("eventDate"),
        )
        .withColumn(
            "isWeekend",
            F.when(
                F.dayofweek("eventDate").isin(1, 7),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "timeIndex",
            F.row_number().over(history_window) - F.lit(1),
        )
    )

    required_history_columns = (
        "lag1",
        "lag7",
        "lag14",
        "rollingMean7",
        "rollingMean28",
        "rollingStd7",
    )

    valid_history = F.col("historyCount28") == F.lit(28)
    for column_name in required_history_columns:
        valid_history = (
            valid_history
            & F.col(column_name).isNotNull()
        )

    return (
        features
        .filter(valid_history)
        .drop("historyCount28")
    )


def chronological_split(
    features: DataFrame,
    date_col: str = "eventDate",
    validation_days: int = 28,
    test_days: int = 28,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    if validation_days <= 0:
        raise ValueError("validation_days must be greater than zero")
    if test_days <= 0:
        raise ValueError("test_days must be greater than zero")

    max_date = (
        features
        .agg(F.max(date_col).alias("maxDate"))
        .first()["maxDate"]
    )

    if max_date is None:
        raise ValueError("features must not be empty")

    test_start = max_date - timedelta(days=test_days - 1)
    validation_start = test_start - timedelta(days=validation_days)

    train = features.filter(
        F.col(date_col) < F.lit(validation_start)
    )
    validation = features.filter(
        (F.col(date_col) >= F.lit(validation_start))
        & (F.col(date_col) < F.lit(test_start))
    )
    test = features.filter(
        F.col(date_col) >= F.lit(test_start)
    )

    return train, validation, test
