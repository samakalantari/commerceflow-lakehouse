from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_dim_date(
    spark: SparkSession,
    start_date: str = "2020-01-01",
    end_date: str = "2030-12-31",
) -> DataFrame:
    """
    Build a date dimension with one row per calendar date.

    Notes:
        - date_sk uses YYYYMMDD format.
        - day_of_week follows Spark numbering:
          1 = Sunday and 7 = Saturday.
    """

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    if start > end:
        raise ValueError(
            "start_date must be before or equal to end_date"
        )

    total_days = (end - start).days + 1

    dates = spark.range(total_days).select(
        F.date_add(
            F.to_date(F.lit(start_date)),
            F.col("id").cast("int"),
        ).alias("full_date")
    )

    return (
        df.withColumn(
            "date_sk",
            F.date_format(
                "full_date",
                "yyyyMMdd",
            ).cast("int"),
        )
        .withColumn(
            "year",
            F.year("full_date"),
        )
        .withColumn(
            "quarter",
            F.quarter("full_date"),
        )
        .withColumn(
            "month",
            F.month("full_date"),
        )
        .withColumn(
            "month_name",
            F.date_format(
                "full_date",
                "MMMM",
            ),
        )
        .withColumn(
            "week_of_year",
            F.weekofyear("full_date"),
        )
        .withColumn(
            "day",
            F.dayofmonth("full_date"),
        )
        .withColumn(
            "day_of_week",
            F.dayofweek("full_date"),
        )
        .withColumn(
            "day_name",
            F.date_format(
                "full_date",
                "EEEE",
            ),
        )
        .withColumn(
            "is_weekend",
            F.dayofweek("full_date").isin(
                1,
                7,
            ),
        )
        .select(
            "date_sk",
            "full_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "week_of_year",
            "day",
            "day_of_week",
            "day_name",
            "is_weekend",
        )
    )
