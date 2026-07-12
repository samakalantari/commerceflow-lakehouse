import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length

from bronze.config.topics import validate_topic
from bronze.sources.kafka_source import read_kafka_stream
from bronze.streaming.checkpoint_manager import build_checkpoint_path


bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

if not bootstrap_servers:
    raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is not set")

topic = validate_topic(
    os.getenv("KAFKA_TOPIC", "transactional.orders")
)

starting_offsets = os.getenv(
    "KAFKA_STARTING_OFFSETS",
    "earliest",
)

max_offsets_per_trigger = int(
    os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "50")
)

checkpoint_base = os.getenv(
    "SPARK_CHECKPOINT_BASE",
    "/opt/spark-data/checkpoints",
)

query_version = os.getenv(
    "STREAM_QUERY_VERSION",
    "v1",
)

test_duration_seconds = int(
    os.getenv("STREAM_TEST_DURATION_SECONDS", "25")
)

safe_topic = topic.replace(".", "_")

spark = (
    SparkSession.builder
    .appName(f"Kafka-Consumer-Test-{safe_topic}")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_df = read_kafka_stream(
    spark=spark,
    bootstrap_servers=bootstrap_servers,
    topic=topic,
    starting_offsets=starting_offsets,
    max_offsets_per_trigger=max_offsets_per_trigger,
)

debug_df = raw_df.select(
    col("kafka_key"),
    col("kafka_topic"),
    col("kafka_partition"),
    col("kafka_offset"),
    col("kafka_timestamp"),
    col("ingested_at"),
    length(col("raw_value")).alias("raw_value_size"),
)

checkpoint_path = build_checkpoint_path(
    base_path=checkpoint_base,
    topic=topic,
    query_name="debug-console",
    version=query_version,
)

print(f"Testing topic: {topic}")
print(f"Checkpoint: {checkpoint_path}")
print(f"Duration: {test_duration_seconds} seconds")

query = (
    debug_df.writeStream
    .format("console")
    .outputMode("append")
    .queryName(f"debug_{safe_topic}")
    .option("truncate", "false")
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="10 seconds")
    .start()
)

try:
    query.awaitTermination(test_duration_seconds)
finally:
    if query.isActive:
        query.stop()

    spark.stop()
    print(f"Test finished: {topic}")
