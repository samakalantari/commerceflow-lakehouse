from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    dayofmonth,
    from_unixtime,
    month,
    to_timestamp,
    when,
    year,
)
from pyspark.sql.types import (
    ByteType,
    IntegerType,
    LongType,
    ShortType,
)

from spark_apps.bronze.config.topic_metadata import get_partition_config

NUMERIC_TIMESTAMP_TYPES = (
    ByteType,
    ShortType,
    IntegerType,
    LongType,
)

EPOCH_MILLISECONDS_THRESHOLD = 100_000_000_000


def _resolve_timestamp_source(
    df: DataFrame,
    topic: str,
) -> str:
    config = get_partition_config(topic)

    if config is None:
        raise ValueError(f"Topic '{topic}' is configured without partitioning.")

    timestamp_field = config["timestamp_field"]

    if timestamp_field in df.columns:
        return timestamp_field

    if "ingested_at" in df.columns:
        print(
            f"[WARN] Partition timestamp field "
            f"'{timestamp_field}' was not found for "
            f"topic '{topic}'. Falling back to "
            "'ingested_at'."
        )

        return "ingested_at"

    raise ValueError(
        "No timestamp column is available for partitioning. "
        f"Configured field '{timestamp_field}' is missing and "
        "fallback field 'ingested_at' is also missing."
    )


def _build_timestamp_expression(
    df: DataFrame,
    source_field: str,
):
    source_type = df.schema[source_field].dataType

    if isinstance(source_type, NUMERIC_TIMESTAMP_TYPES):
        numeric_value = col(source_field)

        epoch_seconds = when(
            numeric_value >= EPOCH_MILLISECONDS_THRESHOLD,
            numeric_value / 1000,
        ).otherwise(numeric_value)

        return to_timestamp(from_unixtime(epoch_seconds))

    return to_timestamp(col(source_field))


def add_time_partitions(df: DataFrame, topic: str) -> DataFrame:
    """
    Add Hive-style time partition columns when the topic
    is configured for time partitioning.

    Topics configured with None are returned unchanged.
    """
    config = get_partition_config(topic)

    if config is None:
        return df

    partition_columns = config["columns"]

    expected_columns = (
        "year",
        "month",
        "day",
    )

    if partition_columns != expected_columns:
        raise ValueError(
            "Timestamp transformation currently supports only "
            "the partition columns: year, month, day. "
            f"Configured for '{topic}': {partition_columns}"
        )

    source_field = _resolve_timestamp_source(
        df=df,
        topic=topic,
    )

    partition_timestamp_column = "__partition_timestamp"

    timestamp_expression = _build_timestamp_expression(
        df=df,
        source_field=source_field,
    )

    return (
        df.withColumn(
            partition_timestamp_column,
            timestamp_expression,
        )
        .withColumn(
            "year",
            year(col(partition_timestamp_column)),
        )
        .withColumn(
            "month",
            month(col(partition_timestamp_column)),
        )
        .withColumn(
            "day",
            dayofmonth(col(partition_timestamp_column)),
        )
        .drop(partition_timestamp_column)
    )
