from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth

try:
    from schemas.topic_schemas import PARTITION_TS_FIELD
except ImportError:
    from spark_apps.bronze.schemas.topic_schemas import PARTITION_TS_FIELD


def add_time_partitions(df, topic):
    ts_field = PARTITION_TS_FIELD.get(topic)

    if ts_field and ts_field in df.columns:
        source_field = ts_field
    else:
        print(f"Warning: Field '{ts_field}' not found for {topic}. Falling back to 'ingested_at'.")
        source_field = "ingested_at"

    partition_ts_col = "__partition_timestamp"

    return (
        df.withColumn(partition_ts_col, to_timestamp(col(source_field)))
          .withColumn("year", year(col(partition_ts_col)))
          .withColumn("month", month(col(partition_ts_col)))
          .withColumn("day", dayofmonth(col(partition_ts_col)))
          .drop(partition_ts_col)
    )
