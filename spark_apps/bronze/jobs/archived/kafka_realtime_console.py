import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "transactional.orders")

if not KAFKA_BOOTSTRAP_SERVERS:
    raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is not set")

spark = (
    SparkSession.builder
    .appName(f"Kafka-Realtime-Console-{TOPIC_NAME.replace('.', '_')}")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "20")
    .load()
)

out = df.select(
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
)

query = (
    out.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", "20")
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"Streaming from Kafka topic: {TOPIC_NAME}")
print("Reading from earliest, then waiting for new messages...")

query.awaitTermination()
