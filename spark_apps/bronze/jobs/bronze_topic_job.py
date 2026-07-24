import os

from pyspark.sql import SparkSession

from spark_apps.bronze.config.minio import (
    configure_minio_storage,
)
from spark_apps.bronze.config.topics import (
    BUSINESS_TOPICS,
    validate_topic,
)
from spark_apps.bronze.decoders.avro_decoder import (
    decode,
)
from spark_apps.bronze.sinks.minio_sink import (
    write_bronze_stream,
)
from spark_apps.bronze.sources.kafka_source import (
    read_kafka_stream,
)
from spark_apps.bronze.transforms.behavioral_transform import (
    group_behavioral_event_fields,
)
from spark_apps.bronze.transforms.timestamp_transform import (
    add_time_partitions,
)


def build_spark(
    app_name: str = "bronze-topic-job",
) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    configure_minio_storage(spark)

    return spark


def build_stream(
    spark: SparkSession,
    topic: str,
):
    """
    Build and start one Bronze stream:

    Kafka
      -> decode
      -> topic-specific transformation
      -> optional partition transformation
      -> MinIO Parquet
    """
    validate_topic(topic)

    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

    checkpoint_base = os.environ["BRONZE_CHECKPOINT_BASE"]

    raw_stream = read_kafka_stream(
        spark=spark,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
    )

    decoded = decode(
        raw_stream,
        topic,
        payload_column="raw_value",
    )

    if decoded is None:
        print(f"[WARN] No schema found for topic '{topic}', skipping.")

        return None

    transformed = group_behavioral_event_fields(
        decoded,
        topic,
    )

    prepared_for_write = add_time_partitions(
        transformed,
        topic,
    )

    query = write_bronze_stream(
        df=prepared_for_write,
        topic=topic,
        checkpoint_base=checkpoint_base,
    )

    checkpoint = f"{checkpoint_base.rstrip('/')}/{topic.replace('.', '/')}"

    print(f"[INFO] Started Bronze stream for topic '{topic}' with checkpoint '{checkpoint}'.")

    return query


def main() -> None:
    spark = build_spark()

    active_queries = []

    for topic in BUSINESS_TOPICS:
        try:
            query = build_stream(
                spark=spark,
                topic=topic,
            )

            if query is not None:
                active_queries.append(query)

        except Exception as exc:
            print(f"[ERROR] Failed to start stream for topic '{topic}': {exc}")

    if not active_queries:
        raise RuntimeError("No Bronze streams were started successfully.")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
