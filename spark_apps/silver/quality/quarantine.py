from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def prepare_quarantine_records(
    df: DataFrame,
    entity_name: str,
    source_topic: str | None = None,
) -> DataFrame:
    """
    Add standard metadata to invalid Silver records.
    """

    source_column = F.coalesce(
        F.col("_dq_source_topic")
        if "_dq_source_topic" in df.columns
        else F.lit(None).cast("string"),
        F.lit(source_topic or "unknown_source"),
    )

    entity_column = F.coalesce(
        F.col("_dq_entity")
        if "_dq_entity" in df.columns
        else F.lit(None).cast("string"),
        F.lit(entity_name),
    )

    partition_column = (
        F.col("kafka_partition").cast("string")
        if "kafka_partition" in df.columns
        else F.lit(None).cast("string")
    )

    offset_column = (
        F.col("kafka_offset").cast("string")
        if "kafka_offset" in df.columns
        else F.lit(None).cast("string")
    )

    return (
        df.withColumn(
            "_dq_quarantine_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    source_column,
                    F.coalesce(
                        partition_column,
                        F.lit("unknown_partition"),
                    ),
                    F.coalesce(
                        offset_column,
                        F.lit("unknown_offset"),
                    ),
                ),
                256,
            ),
        )
        .withColumn(
            "_dq_entity",
            entity_column,
        )
        .withColumn(
            "_dq_source_topic",
            source_column,
        )
        .withColumn(
            "_dq_status",
            F.lit("open"),
        )
        .withColumn(
            "_dq_quarantined_at",
            F.current_timestamp(),
        )
        .dropDuplicates(["_dq_quarantine_id"])
    )


def write_quarantine(
    df: DataFrame,
    table_name: str,
) -> None:
    """Replace the current quarantine state, creating the table on first use."""

    writer = (
        df.writeTo(table_name)
        .using("iceberg")
        .tableProperty("format-version", "2")
    )

    if df.sparkSession.catalog.tableExists(table_name):
        writer.overwrite(F.lit(True))
    else:
        writer.create()
