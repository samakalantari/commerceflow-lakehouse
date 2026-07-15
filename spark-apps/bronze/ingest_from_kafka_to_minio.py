import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from dotenv import load_dotenv

# Import internal modules
from decoders.avro_decoder import decode
from transforms.timestamp_transform import add_time_partitions
from config.topics import BUSINESS_TOPICS, validate_topic

# Load environment variables from .env file
load_dotenv()

# --- Configuration Section ---
# Kafka settings
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
if not KAFKA_BOOTSTRAP:
    raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is not set!")

# MinIO/S3 settings
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]

# Base paths: Use explicit env vars if available, otherwise construct from bucket
BRONZE_BASE_PATH = os.getenv("BRONZE_KAFKA_BASE_PATH", f"s3a://{MINIO_BUCKET}/bronze")
CHECKPOINT_BASE_PATH = os.getenv("BRONZE_CHECKPOINT_BASE", f"s3a://{MINIO_BUCKET}/checkpoints/bronze")

# Initialize Spark Session with S3A support
spark = (
    SparkSession.builder.appName("bronze_ingestion")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

queries = []

# Process each business topic
for topic in BUSINESS_TOPICS:
    # Filter topics using the validation utility
    if not validate_topic(topic):
        print(f"[SKIP] Topic '{topic}' validation failed. Skipping.")
        continue

    # Define unique paths for this topic's data and checkpoints
    topic_subpath = topic.replace('.', '/')
    out_path = f"{BRONZE_BASE_PATH.rstrip('/')}/{topic_subpath}"
    checkpoint_path = f"{CHECKPOINT_BASE_PATH.rstrip('/')}/{topic_subpath}"

    # Setup Kafka stream reader
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Decode Avro payload
    decoded_stream = decode(raw_stream, topic)
    if decoded_stream is None:
        print(f"[SKIP] No schema found for topic '{topic}'. Stream not initiated.")
        continue

    # Add processing timestamp
    enriched_stream = decoded_stream.withColumn("ingested_at", current_timestamp())

    # Add partition columns (year, month, day)
    partitioned_stream = add_time_partitions(enriched_stream, topic)

    # Start the streaming query to MinIO
    query = (
        partitioned_stream.writeStream
        .format("parquet")
        .option("path", out_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("year", "month", "day")
        .trigger(processingTime="30 seconds")
        .start()
    )

    queries.append(query)
    print(f"[START] Started stream for topic: {topic}")

# Wait for all streaming queries
if not queries:
    print("[WARN] No streaming queries were initiated.")
else:
    for q in queries:
        q.awaitTermination()
