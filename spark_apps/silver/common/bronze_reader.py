from __future__ import annotations

import os
from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F




def get_bronze_base_path() -> str:
    """Return the Bronze Kafka base path stored in MinIO."""
    base_path = os.getenv("BRONZE_KAFKA_BASE_PATH")

    if not base_path:
        raise RuntimeError("BRONZE_KAFKA_BASE_PATH is not set.")

    return base_path.rstrip("/")


def bronze_topic_path(topic: str) -> str:
    """Return the active Bronze output path for a topic."""
    base_path = get_bronze_base_path()
    return f"{base_path}/{topic.replace('.', '/')}/new_data"


def bronze_topic_paths(topic: str) -> tuple[str, ...]:
    """Return every Bronze path needed for a complete historical read."""
    base_path = get_bronze_base_path()
    historical_path = f"{base_path}/{topic.replace('.', '/')}/historical_v1"
    return historical_path, bronze_topic_path(topic)


def bronze_topic_historical_path(topic: str) -> str:
    """Return the historical-only Bronze path for a topic."""
    return bronze_topic_paths(topic)[0]


def parse_ingested_date(value: str) -> date:
    """Parse the UTC Bronze ingestion date supplied by Airflow."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("ingested date must use YYYY-MM-DD") from exc


def bronze_topic_day_path(topic: str, ingested_date: date) -> str:
    """Return the raw Bronze-v2 directory for one ingestion date."""
    return (
        f"{bronze_topic_path(topic)}/"
        f"{ingested_date.year:04d}/{ingested_date.month:02d}/{ingested_date.day:02d}"
    )


def split_tombstones(
    df: DataFrame,
    *,
    business_key: str,
    payload_columns: tuple[str, ...],
) -> tuple[DataFrame, DataFrame]:
    """Separate Kafka tombstones from records that require validation."""
    tombstone_condition = F.col("kafka_key").isNotNull()
    for column in payload_columns:
        tombstone_condition = tombstone_condition & F.col(column).isNull()

    tombstones = (
        df.filter(tombstone_condition)
        .select(F.trim(F.col("kafka_key")).alias(business_key))
        .filter(F.col(business_key).isNotNull() & (F.length(business_key) > 0))
        .distinct()
    )
    return df.filter(~tombstone_condition), tombstones


def apply_tombstone_deletes(
    spark: SparkSession,
    *,
    table_name: str,
    business_key: str,
    tombstones: DataFrame,
    view_name: str,
) -> None:
    """Delete daily CDC tombstones from an Iceberg target table."""
    tombstones.createOrReplaceTempView(view_name)
    spark.sql(
        f"""
        MERGE INTO {table_name} AS target
        USING {view_name} AS source
        ON target.{business_key} = source.{business_key}
        WHEN MATCHED THEN DELETE
        """
    )


def _read_bronze_path(
    spark: SparkSession,
    path: str,
) -> DataFrame:
    return spark.read.option("recursiveFileLookup", "true").parquet(path)


def read_bronze_topic(
    spark: SparkSession,
    topic: str,
    *,
    ingested_date: date | None = None,
    source_mode: str | None = None,
) -> DataFrame:
    """Read a daily new_data path, historical_v1, or the legacy combined view."""
    if source_mode == "historical":
        return _read_bronze_path(spark, bronze_topic_historical_path(topic))

    if source_mode not in (None, "daily", "historical"):
        raise ValueError(f"Unsupported Bronze source mode: {source_mode}")
    if ingested_date is not None:
        return _read_bronze_path(
            spark,
            bronze_topic_day_path(topic, ingested_date),
        )

    """Read and concatenate all historical Bronze datasets for a topic."""
    paths = bronze_topic_paths(topic)
    combined_df = _read_bronze_path(spark, paths[0])

    for path in paths[1:]:
        combined_df = combined_df.unionByName(
            _read_bronze_path(spark, path),
            allowMissingColumns=True,
        )

    return combined_df
