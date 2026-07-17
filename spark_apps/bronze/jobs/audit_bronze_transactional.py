import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_apps.bronze.config.minio import configure_minio_storage
from spark_apps.bronze.config.topics import BUSINESS_TOPICS


KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

TRANSACTIONAL_TOPICS = tuple(
    topic
    for topic in BUSINESS_TOPICS
    if topic != "behavioral.events"
)

REQUIRED_COLUMNS = [
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
    "year",
    "month",
    "day",
]


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("audit-bronze-transactional")
        .getOrCreate()
    )

    configure_minio_storage(spark)

    spark.sparkContext.setLogLevel("WARN")

    return spark


def topic_path(topic: str) -> str:
    base_path = os.environ[
        "BRONZE_KAFKA_BASE_PATH"
    ].rstrip("/")

    return (
        f"{base_path}/"
        f"{topic.replace('.', '/')}"
    )


def read_kafka_offsets(
    spark: SparkSession,
    topic: str,
) -> DataFrame:
    return (
        spark.read
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option(
            "subscribe",
            topic,
        )
        .option(
            "startingOffsets",
            "earliest",
        )
        .option(
            "endingOffsets",
            "latest",
        )
        .load()
        .select(
            F.col("topic").alias(
                "kafka_topic"
            ),
            F.col("partition").alias(
                "kafka_partition"
            ),
            F.col("offset").alias(
                "kafka_offset"
            ),
        )
    )


def audit_topic(
    spark: SparkSession,
    topic: str,
) -> dict:
    path = topic_path(topic)

    print("\n")
    print("=" * 110)
    print(f"TOPIC: {topic}")
    print(f"BRONZE PATH: {path}")
    print("=" * 110)

    errors = []
    warnings = []

    # ---------------------------------------------------------
    # Read Kafka first
    # ---------------------------------------------------------

    try:
        kafka_offsets = (
            read_kafka_offsets(
                spark,
                topic,
            )
            .cache()
        )

        kafka_count = kafka_offsets.count()

    except Exception as exc:
        print(
            f"[FAIL] Could not read "
            f"Kafka topic '{topic}'."
        )
        print(f"Reason: {exc}")

        return {
            "topic": topic,
            "status": "FAIL",
            "bronze_count": None,
            "kafka_count": None,
            "missing": None,
            "duplicates": None,
        }

    print(
        f"Kafka records "
        f"(earliest -> latest): "
        f"{kafka_count:,}"
    )

    # ---------------------------------------------------------
    # Read Bronze
    # ---------------------------------------------------------

    try:
        bronze_df = (
            spark.read
            .parquet(path)
            .cache()
        )

        bronze_count = bronze_df.count()

    except Exception as exc:

        # Empty Kafka topic may legitimately
        # have no Bronze Parquet output yet.
        if kafka_count == 0:
            print(
                "[PASS] Kafka topic is empty "
                "and no Bronze output exists."
            )

            return {
                "topic": topic,
                "status": "PASS",
                "bronze_count": 0,
                "kafka_count": 0,
                "missing": 0,
                "duplicates": 0,
            }

        print(
            f"[FAIL] Could not read "
            f"Bronze output for '{topic}'."
        )
        print(f"Reason: {exc}")

        return {
            "topic": topic,
            "status": "FAIL",
            "bronze_count": None,
            "kafka_count": kafka_count,
            "missing": None,
            "duplicates": None,
        }

    print(
        f"Bronze records: "
        f"{bronze_count:,}"
    )

    # ---------------------------------------------------------
    # Required columns
    # ---------------------------------------------------------

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in bronze_df.columns
    ]

    if missing_columns:
        errors.append(
            "Missing required columns: "
            + ", ".join(
                missing_columns
            )
        )

        print(
            f"[FAIL] Missing columns: "
            f"{missing_columns}"
        )

        return {
            "topic": topic,
            "status": "FAIL",
            "bronze_count": bronze_count,
            "kafka_count": kafka_count,
            "missing": None,
            "duplicates": None,
        }

    # ---------------------------------------------------------
    # Kafka topic consistency
    # ---------------------------------------------------------

    wrong_topic_count = (
        bronze_df
        .filter(
            F.col("kafka_topic")
            != topic
        )
        .count()
    )

    if wrong_topic_count > 0:
        errors.append(
            f"{wrong_topic_count} records "
            "have incorrect kafka_topic"
        )

    # ---------------------------------------------------------
    # Null Kafka metadata
    # ---------------------------------------------------------

    null_metadata_count = (
        bronze_df
        .filter(
            F.col(
                "kafka_topic"
            ).isNull()
            |
            F.col(
                "kafka_partition"
            ).isNull()
            |
            F.col(
                "kafka_offset"
            ).isNull()
            |
            F.col(
                "kafka_timestamp"
            ).isNull()
            |
            F.col(
                "ingested_at"
            ).isNull()
        )
        .count()
    )

    if null_metadata_count > 0:
        errors.append(
            f"{null_metadata_count} records "
            "have null Kafka metadata"
        )

    # ---------------------------------------------------------
    # Duplicate Kafka messages
    # ---------------------------------------------------------

    bronze_offsets = (
        bronze_df
        .select(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
        )
        .distinct()
        .cache()
    )

    distinct_offset_count = (
        bronze_offsets.count()
    )

    duplicate_count = (
        bronze_count
        - distinct_offset_count
    )

    if duplicate_count > 0:
        errors.append(
            f"{duplicate_count} duplicate "
            "Kafka offsets found"
        )

    # ---------------------------------------------------------
    # Partition date validation
    # ---------------------------------------------------------

    null_partition_date_count = (
        bronze_df
        .filter(
            F.col("year").isNull()
            |
            F.col("month").isNull()
            |
            F.col("day").isNull()
        )
        .count()
    )

    invalid_partition_date_count = (
        bronze_df
        .filter(
            (
                F.col("year")
                != F.year(
                    "ingested_at"
                )
            )
            |
            (
                F.col("month")
                != F.month(
                    "ingested_at"
                )
            )
            |
            (
                F.col("day")
                != F.dayofmonth(
                    "ingested_at"
                )
            )
        )
        .count()
    )

    if null_partition_date_count > 0:
        errors.append(
            f"{null_partition_date_count} "
            "records have null "
            "year/month/day"
        )

    if invalid_partition_date_count > 0:
        errors.append(
            f"{invalid_partition_date_count} "
            "records have incorrect "
            "year/month/day"
        )

    # ---------------------------------------------------------
    # Compare exact Kafka offsets with Bronze
    # ---------------------------------------------------------

    missing_from_bronze = (
        kafka_offsets
        .join(
            bronze_offsets,
            on=[
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
            ],
            how="left_anti",
        )
        .count()
    )

    if missing_from_bronze > 0:
        errors.append(
            f"{missing_from_bronze} "
            "current Kafka messages "
            "are missing from Bronze"
        )

    # ---------------------------------------------------------
    # Bronze records no longer visible in Kafka
    # ---------------------------------------------------------

    bronze_only_count = (
        bronze_offsets
        .join(
            kafka_offsets,
            on=[
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
            ],
            how="left_anti",
        )
        .count()
    )

    if bronze_only_count > 0:
        warnings.append(
            f"{bronze_only_count} Bronze "
            "offsets are not present in "
            "the current Kafka retention window"
        )

    # ---------------------------------------------------------
    # Offset summary
    # ---------------------------------------------------------

    print("\nBRONZE OFFSET SUMMARY")
    print("-" * 110)

    (
        bronze_df
        .groupBy(
            "kafka_partition"
        )
        .agg(
            F.count("*").alias(
                "records"
            ),
            F.countDistinct(
                "kafka_offset"
            ).alias(
                "distinct_offsets"
            ),
            F.min(
                "kafka_offset"
            ).alias(
                "min_offset"
            ),
            F.max(
                "kafka_offset"
            ).alias(
                "max_offset"
            ),
        )
        .orderBy(
            "kafka_partition"
        )
        .show(
            truncate=False
        )
    )

    print("\nKAFKA OFFSET SUMMARY")
    print("-" * 110)

    (
        kafka_offsets
        .groupBy(
            "kafka_partition"
        )
        .agg(
            F.count("*").alias(
                "records"
            ),
            F.min(
                "kafka_offset"
            ).alias(
                "min_offset"
            ),
            F.max(
                "kafka_offset"
            ).alias(
                "max_offset"
            ),
        )
        .orderBy(
            "kafka_partition"
        )
        .show(
            truncate=False
        )
    )

    # ---------------------------------------------------------
    # Final topic result
    # ---------------------------------------------------------

    print("\nAUDIT RESULT")
    print("-" * 110)

    print(
        f"Bronze records:              "
        f"{bronze_count:,}"
    )

    print(
        f"Kafka current records:       "
        f"{kafka_count:,}"
    )

    print(
        f"Duplicate offsets:           "
        f"{duplicate_count:,}"
    )

    print(
        f"Missing from Bronze:         "
        f"{missing_from_bronze:,}"
    )

    print(
        f"Bronze-only offsets:         "
        f"{bronze_only_count:,}"
    )

    print(
        f"Null Kafka metadata:         "
        f"{null_metadata_count:,}"
    )

    print(
        f"Wrong kafka_topic:           "
        f"{wrong_topic_count:,}"
    )

    print(
        f"Invalid date partitions:     "
        f"{invalid_partition_date_count:,}"
    )

    for warning in warnings:
        print(
            f"[WARN] {warning}"
        )

    if errors:
        for error in errors:
            print(
                f"[FAIL] {error}"
            )

        status = "FAIL"

    else:
        print(
            "[PASS] Bronze topic "
            "passed all checks."
        )

        status = "PASS"

    bronze_df.unpersist()
    bronze_offsets.unpersist()
    kafka_offsets.unpersist()

    return {
        "topic": topic,
        "status": status,
        "bronze_count": bronze_count,
        "kafka_count": kafka_count,
        "missing": missing_from_bronze,
        "duplicates": duplicate_count,
    }


def main() -> None:
    spark = build_spark()

    results = []

    try:
        for topic in TRANSACTIONAL_TOPICS:
            try:
                result = audit_topic(
                    spark,
                    topic,
                )

            except Exception as exc:
                print("\n")
                print("=" * 110)
                print(
                    f"[FAIL] Unexpected "
                    f"error for {topic}"
                )
                print(str(exc))

                result = {
                    "topic": topic,
                    "status": "FAIL",
                    "bronze_count": None,
                    "kafka_count": None,
                    "missing": None,
                    "duplicates": None,
                }

            results.append(
                result
            )

        print("\n")
        print("=" * 110)
        print(
            "FINAL BRONZE AUDIT SUMMARY"
        )
        print("=" * 110)

        for result in results:
            print(
                f"{result['status']:5} | "
                f"{result['topic']:40} | "
                f"Bronze="
                f"{result['bronze_count']} | "
                f"Kafka="
                f"{result['kafka_count']} | "
                f"Missing="
                f"{result['missing']} | "
                f"Duplicates="
                f"{result['duplicates']}"
            )

        failed_topics = [
            result["topic"]
            for result in results
            if result["status"]
            != "PASS"
        ]

        print("\n")
        print("=" * 110)

        if failed_topics:
            print(
                "BRONZE AUDIT FAILED"
            )

            print(
                "Topics requiring "
                "investigation:"
            )

            for topic in failed_topics:
                print(
                    f" - {topic}"
                )

        else:
            print(
                "ALL TRANSACTIONAL "
                "BRONZE TOPICS PASSED"
            )

            print(
                "Bronze is ready "
                "for Silver processing."
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
