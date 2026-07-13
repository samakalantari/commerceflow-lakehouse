import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "transactional.orders")

if not KAFKA_BOOTSTRAP_SERVERS:
    raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is not set")

spark = (
    SparkSession.builder
    .appName("Kafka-Test-Reader")
    .getOrCreate()
)

df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

result = df.select(
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
)

print(f"Reading Kafka topic: {TOPIC_NAME}")
print(f"Kafka bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")

result.show(10, truncate=False)

print("Kafka read test finished successfully.")

spark.stop()
