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

try:
    from schemas.topic_schemas import PARTITION_TS_FIELD
except ImportError:
    from spark_apps.bronze.schemas.topic_schemas import PARTITION_TS_FIELD


NUMERIC_TIMESTAMP_TYPES = (
    ByteType,
    ShortType,
    IntegerType,
    LongType,
)

EPOCH_MILLISECONDS_THRESHOLD = 100_000_000_000


def _resolve_timestamp_source(df, topic):
    ts_field = PARTITION_TS_FIELD.get(topic)

    if ts_field is None:
        raise ValueError(
            f"Unsupported topic for timestamp partitioning: {topic}"
        )

    if ts_field in df.columns:
        return ts_field

    if "ingested_at" in df.columns:
        print(
            f"Warning: Field '{ts_field}' not found for {topic}. "
            "Falling back to 'ingested_at'."
        )
        return "ingested_at"

    raise ValueError(
        "No timestamp column available for partitioning. "
        f"Configured field '{ts_field}' is missing and "
        "fallback field 'ingested_at' is also missing."
    )


def _build_timestamp_expression(df, source_field):
    source_type = df.schema[source_field].dataType

    if isinstance(source_type, NUMERIC_TIMESTAMP_TYPES):
        numeric_value = col(source_field)

        epoch_seconds = when(
            numeric_value >= EPOCH_MILLISECONDS_THRESHOLD,
            numeric_value / 1000,
        ).otherwise(numeric_value)

        return to_timestamp(
            from_unixtime(epoch_seconds)
        )

    return to_timestamp(
        col(source_field)
    )


def add_time_partitions(df, topic):
    source_field = _resolve_timestamp_source(
        df,
        topic,
    )

    partition_ts_col = "__partition_timestamp"

    timestamp_expression = _build_timestamp_expression(
        df,
        source_field,
    )

    return (
        df.withColumn(
            partition_ts_col,
            timestamp_expression,
        )
        .withColumn(
            "year",
            year(col(partition_ts_col)),
        )
        .withColumn(
            "month",
            month(col(partition_ts_col)),
        )
        .withColumn(
            "day",
            dayofmonth(col(partition_ts_col)),
        )
        .drop(partition_ts_col)
    )