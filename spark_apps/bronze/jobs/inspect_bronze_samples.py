import argparse
import os
import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, struct, to_json

from spark_apps.bronze.config.minio import configure_minio_storage
from spark_apps.bronze.config.topics import BUSINESS_TOPICS


def build_spark() -> SparkSession:
    """
    Create a Spark session configured for reading files from MinIO.
    """
    spark = (
        SparkSession.builder
        .appName("inspect-bronze-samples")
        .getOrCreate()
    )

    configure_minio_storage(spark)

    spark.sparkContext.setLogLevel("WARN")

    return spark


def topic_path(topic: str) -> str:
    """
    Convert a Kafka topic name to its Bronze MinIO path.

    Example:
        behavioral.events
        ->
        s3a://commerceflow-lakehouse/bronze/behavioral/events
    """
    base_path = os.environ["BRONZE_KAFKA_BASE_PATH"].rstrip("/")

    return f"{base_path}/{topic.replace('.', '/')}"


def order_latest_first(df: DataFrame) -> DataFrame:
    """
    Sort records by Kafka timestamp or ingestion timestamp when available.
    """
    if "kafka_timestamp" in df.columns:
        return df.orderBy(
            col("kafka_timestamp").desc()
        )

    if "ingested_at" in df.columns:
        return df.orderBy(
            col("ingested_at").desc()
        )

    return df


def print_topic_sample(
    spark: SparkSession,
    topic: str,
    limit: int,
) -> None:
    """
    Read one topic's Bronze Parquet output and display sample records.
    """
    path = topic_path(topic)

    print("\n")
    print("=" * 100)
    print(f"TOPIC: {topic}")
    print(f"PATH: {path}")
    print("=" * 100)

    try:
        df = spark.read.parquet(path)
    except Exception as exc:
        print(
            f"[ERROR] Could not read Bronze output "
            f"for topic '{topic}'."
        )
        print(f"Reason: {exc}")
        return

    print("\nSCHEMA")
    print("-" * 100)
    df.printSchema()

    sample_df = (
        order_latest_first(df)
        .limit(limit)
    )

    sample_count = sample_df.count()

    print("\nSAMPLE RECORDS")
    print("-" * 100)
    print(f"Records displayed: {sample_count}")

    if sample_count == 0:
        print(
            "[WARN] No records found for this topic."
        )
        return

    if topic == "behavioral.events":
        json_df = sample_df.select(
            to_json(
                struct(
                    col("timestamp").alias(
                        "event_timestamp"
                    ),
                    col("user_id"),
                    col("event_type"),
                    col("device"),
                    col("session_id"),
                    col("event_data"),
                ),
                options={
                    "ignoreNullFields": "true",
                },
            ).alias("event_json")
        )

        rows = json_df.collect()

        for index, row in enumerate(rows):
            print()
            print(f"RECORD {index}")
            print("-" * 100)

            parsed = json.loads(
                row.event_json
            )

            print(
                json.dumps(
                    parsed,
                    indent=2,
                    ensure_ascii=False,
                )
            )

        return

    sample_df.show(
        n=limit,
        truncate=False,
        vertical=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Read Bronze Parquet output from MinIO "
            "and display sample records."
        )
    )

    parser.add_argument(
        "--topic",
        default="all",
        choices=["all", *BUSINESS_TOPICS],
        help=(
            "Topic to inspect. "
            "Use 'all' to inspect every configured topic."
        ),
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of records to display per topic.",
    )

    args = parser.parse_args()

    if args.limit <= 0:
        parser.error("--limit must be greater than zero")

    spark = build_spark()

    try:
        topics = (
            BUSINESS_TOPICS
            if args.topic == "all"
            else (args.topic,)
        )

        for topic in topics:
            print_topic_sample(
                spark=spark,
                topic=topic,
                limit=args.limit,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()