"""Read-only audit of Bronze original/recovery paths."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Iterable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_apps.bronze.config.minio import configure_minio_storage
from spark_apps.bronze.config.topics import BUSINESS_TOPICS, validate_topic

KAFKA_IDENTITY = ["kafka_topic", "kafka_partition", "kafka_offset"]
RECOVERY_TOPICS = {
    "transactional.orders",
    "transactional.product_price_history",
    "transactional.users",
}
PARTITION_TIMESTAMP_FIELDS = {
    "transactional.categories": "ingested_at",
    "transactional.order_items": "ingested_at",
    "transactional.orders": "timestamp",
    "transactional.product_price_history": "valid_from",
    "transactional.products": "ingested_at",
    "transactional.returns_refunds": "return_timestamp",
    "transactional.users": "signup_date",
    "behavioral.events": "timestamp",
}
NON_PAYLOAD_COLUMNS = {
    "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset",
    "kafka_timestamp", "ingested_at", "year", "month", "day",
    "_bronze_source_path", "_bronze_generation", "_bronze_priority",
    "_bronze_payload_hash", "_bronze_partition_timestamp",
    "_bronze_partition_ts_source", "_bronze_business_ts_quality",
}


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("audit-bronze-original-recovery")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    configure_minio_storage(spark)
    spark.sparkContext.setLogLevel("WARN")
    return spark


def path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return bool(fs.exists(hadoop_path))


def original_path(topic: str) -> str:
    base = os.environ["BRONZE_KAFKA_BASE_PATH"].rstrip("/")
    return f"{base}/{topic.replace('.', '/')}"


def recovery_path(topic: str) -> Optional[str]:
    if topic not in RECOVERY_TOPICS:
        return None
    return f"{original_path(topic)}_recovery"


def read_source(
    spark: SparkSession,
    *,
    path: str,
    generation: str,
    priority: int,
) -> DataFrame:
    return (
        spark.read.option("mergeSchema", "true").parquet(path)
        .withColumn("_bronze_source_path", F.lit(path))
        .withColumn("_bronze_generation", F.lit(generation))
        .withColumn("_bronze_priority", F.lit(priority).cast("int"))
    )


def require_identity(df: DataFrame, label: str) -> None:
    missing = [c for c in KAFKA_IDENTITY if c not in df.columns]
    if missing:
        raise ValueError(f"{label} missing Kafka identity columns: {missing}")


def payload_columns(df: DataFrame) -> list[str]:
    return sorted(c for c in df.columns if c not in NON_PAYLOAD_COLUMNS)


def with_payload_hash(df: DataFrame) -> DataFrame:
    columns = payload_columns(df)
    if not columns:
        return df.withColumn("_bronze_payload_hash", F.sha2(F.lit("{}"), 256))
    return df.withColumn(
        "_bronze_payload_hash",
        F.sha2(
            F.to_json(
                F.struct(*[F.col(c).alias(c) for c in columns]),
                options={"ignoreNullFields": "false"},
            ),
            256,
        ),
    )


def timestamp_expression(df: DataFrame, field: str):
    if field not in df.columns:
        return F.lit(None).cast("timestamp")
    dtype = df.schema[field].dataType
    source = F.col(field)
    if isinstance(dtype, T.TimestampType):
        return source
    if isinstance(dtype, T.DateType):
        return source.cast("timestamp")
    if isinstance(
        dtype,
        (
            T.ByteType, T.ShortType, T.IntegerType, T.LongType,
            T.FloatType, T.DoubleType, T.DecimalType,
        ),
    ):
        numeric = source.cast("double")
        seconds = F.when(
            F.abs(numeric) >= F.lit(100_000_000_000),
            numeric / F.lit(1000.0),
        ).otherwise(numeric)
        return F.to_timestamp(F.from_unixtime(seconds.cast("long")))
    return F.to_timestamp(source.cast("string"))


def offset_summary(df: DataFrame) -> list[dict[str, Any]]:
    rows = (
        df.groupBy("kafka_topic", "kafka_partition")
        .agg(
            F.min("kafka_offset").alias("min_offset"),
            F.max("kafka_offset").alias("max_offset"),
            F.count("*").alias("row_count"),
            F.countDistinct("kafka_offset").alias("distinct_offset_count"),
        )
        .orderBy("kafka_topic", "kafka_partition")
        .collect()
    )
    return [row.asDict(recursive=True) for row in rows]


def audit_topic(
    spark: SparkSession,
    *,
    topic: str,
    minimum_valid_year: int,
) -> dict[str, Any]:
    validate_topic(topic)
    sources: list[DataFrame] = []
    source_report: dict[str, Any] = {}

    for generation, path, priority in (
        ("original", original_path(topic), 1),
        ("recovery", recovery_path(topic), 2),
    ):
        if not path or not path_exists(spark, path):
            source_report[generation] = {
                "path": path,
                "exists": False,
                "count": 0,
                "duplicate_identity_rows": 0,
            }
            continue
        df = read_source(
            spark,
            path=path,
            generation=generation,
            priority=priority,
        ).cache()
        require_identity(df, generation)
        count = df.count()
        distinct_count = df.select(*KAFKA_IDENTITY).distinct().count()
        source_report[generation] = {
            "path": path,
            "exists": True,
            "count": count,
            "duplicate_identity_rows": count - distinct_count,
            "offsets": offset_summary(df),
        }
        sources.append(df)

    if not sources:
        return {"topic": topic, "status": "NO_SOURCE_DATA", "sources": source_report}

    combined = sources[0]
    for df in sources[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)
    combined = with_payload_hash(combined).cache()
    require_identity(combined, "combined")

    combined_count = combined.count()
    distinct_count = combined.select(*KAFKA_IDENTITY).distinct().count()
    duplicate_rows = combined_count - distinct_count

    overlap_count = 0
    if len(sources) > 1:
        overlap_count = (
            combined.groupBy(*KAFKA_IDENTITY)
            .agg(F.countDistinct("_bronze_generation").alias("generations"))
            .filter(F.col("generations") > 1)
            .count()
        )

    conflict_count = (
        combined.groupBy(*KAFKA_IDENTITY)
        .agg(F.countDistinct("_bronze_payload_hash").alias("versions"))
        .filter(F.col("versions") > 1)
        .count()
    )

    field = PARTITION_TIMESTAMP_FIELDS[topic]
    parsed = timestamp_expression(combined, field)
    ts_report = (
        combined.select(parsed.alias("_audit_ts"))
        .agg(
            F.sum(F.when(F.col("_audit_ts").isNull(), 1).otherwise(0)).alias("null_or_unparseable"),
            F.sum(F.when(F.year("_audit_ts") == 1970, 1).otherwise(0)).alias("year_1970"),
            F.sum(
                F.when(
                    F.col("_audit_ts").isNotNull()
                    & (F.year("_audit_ts") < minimum_valid_year),
                    1,
                ).otherwise(0)
            ).alias("too_old"),
            F.sum(
                F.when(
                    F.col("_audit_ts") > F.current_timestamp() + F.expr("INTERVAL 1 DAY"),
                    1,
                ).otherwise(0)
            ).alias("future"),
            F.min("_audit_ts").alias("minimum"),
            F.max("_audit_ts").alias("maximum"),
        )
        .collect()[0]
        .asDict(recursive=True)
    )

    partition_mismatch = None
    if {"year", "month", "day"}.issubset(set(combined.columns)):
        partition_mismatch = (
            combined.filter(
                parsed.isNull()
                | (F.col("year") != F.year(parsed))
                | (F.col("month") != F.month(parsed))
                | (F.col("day") != F.dayofmonth(parsed))
            ).count()
        )

    status = "PASS"
    if duplicate_rows > 0 or conflict_count > 0:
        status = "REVIEW_REQUIRED"

    return {
        "topic": topic,
        "status": status,
        "partition_timestamp_field": field,
        "sources": source_report,
        "combined": {
            "row_count": combined_count,
            "distinct_kafka_identity_count": distinct_count,
            "duplicate_identity_rows": duplicate_rows,
            "overlap_identity_count": overlap_count,
            "conflicting_identity_count": conflict_count,
            "offsets": offset_summary(combined),
        },
        "timestamp_quality": ts_report,
        "partition_mismatch_count": partition_mismatch,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only Bronze source audit")
    parser.add_argument("--topic", action="append", choices=BUSINESS_TOPICS)
    parser.add_argument("--minimum-valid-year", type=int, default=2000)
    parser.add_argument("--report-file", help="Optional local JSON file on the driver")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark()
    topics: Iterable[str] = args.topic or BUSINESS_TOPICS
    reports = []
    try:
        for topic in topics:
            report = audit_topic(
                spark,
                topic=topic,
                minimum_valid_year=args.minimum_valid_year,
            )
            reports.append(report)
            print(json.dumps(report, indent=2, default=str))
    finally:
        spark.stop()

    if args.report_file:
        destination = Path(args.report_file)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            json.dumps({"reports": reports}, indent=2, default=str),
            encoding="utf-8",
        )
        print(f"[INFO] Local report written: {destination}")


if __name__ == "__main__":
    main()
