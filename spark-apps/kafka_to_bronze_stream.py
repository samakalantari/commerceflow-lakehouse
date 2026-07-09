import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "transactional.orders")

if not KAFKA_BOOTSTRAP_SERVERS:
    raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is not set")

safe_topic_name = TOPIC_NAME.replace(".", "_")

bronze_path = f"/opt/spark-data/bronze/{safe_topic_name}"
checkpoint_path = f"/opt/spark-data/checkpoints/{safe_topic_name}"

spark = (
    SparkSession.builder
    .appName(f"Kafka-To-Bronze-{safe_topic_name}")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "1000")
    .load()
)

bronze_df = raw_df.select(
    col("key").cast("string").alias("message_key"),
    col("value").alias("message_value_binary"),
    col("value").cast("string").alias("message_value_string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    col("timestampType").alias("kafka_timestamp_type"),
    current_timestamp().alias("ingested_at"),
)

query = (
    bronze_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", bronze_path)
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"Started Kafka consumer for topic: {TOPIC_NAME}", flush=True)
print(f"Bronze path: {bronze_path}", flush=True)
print(f"Checkpoint path: {checkpoint_path}", flush=True)
print("Reading from earliest if checkpoint does not exist, then waiting for new Kafka messages...", flush=True)

query.awaitTermination()
