# spark-apps/bronze/jobs/bronze_topic_job.py
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp

from spart_app.bronze.config.minio import configure_minio_storage
from spart_app.bronze.config.topics import BUSINESS_TOPICS, validate_topic
from spart_app.bronze.schemas.topic_schemas import PARTITION_TS_FIELD
from spart_app.bronze.sources.kafka_source import read_kafka_stream
from spart_app.bronze.decoders.avro_decoder import decode
from spart_app.bronze.sinks.minio_sink import write_bronze_stream


def build_spark(app_name: str = "bronze-topic-job") -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    configure_minio_storage(spark)
    return spark


def build_stream(spark: SparkSession, topic: str):
    """Read one Kafka topic, decode Avro, apply partitioning, start the write stream."""
    validate_topic(topic)
    event_time_field = PARTITION_TS_FIELD[topic]

    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    checkpoint_base = os.environ["BRONZE_CHECKPOINT_BASE"]

    raw_stream = read_kafka_stream(spark, bootstrap_servers, topic)

    # kafka_source aliases the Kafka `value` column as `raw_value` (still carrying
    # the 5-byte Confluent wire-format header). avro_decoder.decode() strips the
    # header, fetches the schema from Schema Registry and flattens the fields
    # directly onto the DataFrame.
    decoded = decode(raw_stream, topic, payload_column="raw_value")

    if decoded is None:
        # Schema Registry has no registered subject for this topic yet
        # (e.g. no message has been produced on it). Skip it for now instead
        # of crashing the whole job — it can be picked up on a later restart.
        print(f"[WARN] No schema found for topic '{topic}', skipping.")
        return None

    parsed = (
        decoded
        .withColumn(event_time_field, to_timestamp(col(event_time_field)))
        .withColumn("event_date", to_date(col(event_time_field)))
        .drop("raw_value")
    )

    query = write_bronze_stream(parsed, topic, checkpoint_base)
    print(f"[INFO] Started stream for topic '{topic}' -> checkpoint={checkpoint_base}/{topic.replace('.', '/')}")
    return query


def main():
    spark = build_spark()

    active_queries = []
    for topic in BUSINESS_TOPICS:
        try:
            query = build_stream(spark, topic)
            if query is not None:
                active_queries.append(query)
        except Exception as exc:
            print(f"[ERROR] Failed to start stream for topic '{topic}': {exc}")

    if not active_queries:
        raise RuntimeError("No streams were started successfully. Aborting job.")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
