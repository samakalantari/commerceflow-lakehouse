from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp


def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "earliest",
    max_offsets_per_trigger: Optional[int] = 10_000,
) -> DataFrame:
    if not bootstrap_servers:
        raise ValueError("bootstrap_servers must not be empty")

    if not topic:
        raise ValueError("topic must not be empty")

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "true")
    )

    if max_offsets_per_trigger is not None:
        reader = reader.option(
            "maxOffsetsPerTrigger",
            str(max_offsets_per_trigger),
        )

    return reader.load().select(
        col("key").cast("string").alias("kafka_key"),
        col("value").alias("raw_value"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingested_at"),
    )
