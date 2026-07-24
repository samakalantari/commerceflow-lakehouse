from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def prepare_quarantine_records(
    df: DataFrame,
    entity_name: str,
    source_topic: str,
) -> DataFrame:
    """
    Add standard metadata to invalid Silver records.

    The input DataFrame must contain:
        _dq_error_reason
        kafka_partition
        kafka_offset
    """
    return (
        df.withColumn(
            "_dq_quarantine_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit(source_topic),
                    F.coalesce(
                        F.col("kafka_partition").cast("string"),
                        F.lit("unknown_partition"),
                    ),
                    F.coalesce(
                        F.col("kafka_offset").cast("string"),
                        F.lit("unknown_offset"),
                    ),
                ),
                256,
            ),
        )
        .withColumn(
            "_dq_entity",
            F.lit(entity_name),
        )
        .withColumn(
            "_dq_source_topic",
            F.lit(source_topic),
        )
        .withColumn(
            "_dq_status",
            F.lit("open"),
        )
        .withColumn(
            "_dq_quarantined_at",
            F.current_timestamp(),
        )
    )


def write_quarantine(
    df: DataFrame,
    table_name: str,
) -> None:
    """
    Append new invalid records to an Iceberg quarantine table.
    """
    if df.isEmpty():
        return

    (
        df.writeTo(table_name)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .append()
    )