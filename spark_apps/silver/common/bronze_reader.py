from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession




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


def _read_bronze_path(
    spark: SparkSession,
    path: str,
) -> DataFrame:
    return spark.read.option("recursiveFileLookup", "true").parquet(path)


def read_bronze_topic(
    spark: SparkSession,
    topic: str,
) -> DataFrame:
    """Read and concatenate all historical Bronze datasets for a topic."""
    paths = bronze_topic_paths(topic)
    combined_df = _read_bronze_path(spark, paths[0])

    for path in paths[1:]:
        combined_df = combined_df.unionByName(
            _read_bronze_path(spark, path),
            allowMissingColumns=True,
        )

    return combined_df
