import os
from typing import Optional

from pyspark.sql import DataFrame

from spark_apps.bronze.config.topic_metadata import (
    get_partition_columns,
)


def _validate_partition_columns(
    df: DataFrame,
    partition_columns: tuple[str, ...],
) -> None:

    if not partition_columns:
        return

    missing_columns = [column for column in partition_columns if column not in df.columns]

    if missing_columns:
        missing = ", ".join(missing_columns)

        raise ValueError(f"Missing partition columns for Bronze write: {missing}")


def write_bronze_stream(df: DataFrame, topic: str, checkpoint_base: str):
    partition_columns = get_partition_columns(topic)

    _validate_partition_columns(df=df, partition_columns=partition_columns)

    path = _topic_to_path(topic=topic)

    checkpoint = _topic_to_checkpoint(checkpoint_base=checkpoint_base, topic=topic)

    writer = (
        df.writeStream.format("parquet")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
    )

    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    return writer.start()


def write_bronze_batch(
    df: DataFrame, topic: str, output_base_path: str, mode: str = "errorifexists"
) -> str:
    partition_columns = get_partition_columns(topic)

    _validate_partition_columns(df=df, partition_columns=partition_columns)

    output_path = _topic_to_path(topic=topic, base_path=output_base_path)

    writer = df.write.format("parquet").mode(mode)

    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    writer.save(output_path)

    return output_path


def _topic_to_path(
    topic: str,
    base_path: Optional[str] = None,
) -> str:
    if base_path is None:
        base_path = os.environ["BRONZE_KAFKA_BASE_PATH"]

    base = base_path.rstrip("/")

    topic_path = topic.replace(".", "/")

    return f"{base}/{topic_path}"


def _topic_to_checkpoint(checkpoint_base: str, topic: str) -> str:
    base = checkpoint_base.rstrip("/")

    topic_path = topic.replace(
        ".",
        "/",
    )

    return f"{base}/{topic_path}"
