"""Build a canonical, non-destructive Bronze historical_v1 dataset."""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession, Window
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
    "transactional.orders": "ingested_at",
    "transactional.product_price_history": "ingested_at",
    "transactional.products": "ingested_at",
    "transactional.returns_refunds": "ingested_at",
    "transactional.users": "ingested_at",
    "behavioral.events": "ingested_at",
}
HASH_EXCLUDED = {
    "year", "month", "day", "_bronze_source_path", "_bronze_generation",
    "_bronze_priority", "_bronze_payload_hash", "_bronze_partition_timestamp",
    "_bronze_partition_ts_source", "_bronze_business_ts_quality",
}


def build_spark(topic: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(f"build-bronze-historical-v1-{topic.replace('.', '-')}")
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


def output_path(topic: str) -> str:
    base = os.getenv("BRONZE_V2_BASE_PATH")
    if not base:
        raise ValueError("Provide --output-path or set BRONZE_V2_BASE_PATH")
    return f"{base.rstrip('/')}/{topic.replace('.', '/')}/historical_v1"


def read_source(
    spark: SparkSession,
    *,
    path: str,
    generation: str,
    priority: int,
) -> DataFrame:
    return (
        spark.read.option("mergeSchema", "true").parquet(f"{path.rstrip('/')}/year=*/month=*/day=*")
        .withColumn("_bronze_source_path", F.lit(path))
        .withColumn("_bronze_generation", F.lit(generation))
        .withColumn("_bronze_priority", F.lit(priority).cast("int"))
    )


def require_columns(df: DataFrame, columns: list[str], label: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def hash_columns(df: DataFrame) -> list[str]:
    return sorted(column for column in df.columns if column not in HASH_EXCLUDED)


def add_payload_hash(df: DataFrame, columns: Optional[list[str]] = None) -> DataFrame:
    selected = columns or hash_columns(df)
    return df.withColumn(
        "_bronze_payload_hash",
        F.sha2(
            F.to_json(
                F.struct(*[F.col(column).alias(column) for column in selected]),
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


def valid_timestamp(ts, minimum_valid_year: int, maximum_future_days: int):
    return (
        ts.isNotNull()
        & (F.year(ts) >= F.lit(minimum_valid_year))
        & (
            ts <= F.current_timestamp()
            + F.expr(f"INTERVAL {int(maximum_future_days)} DAYS")
        )
    )


def add_safe_partitions(
    df: DataFrame,
    *,
    topic: str,
    minimum_valid_year: int,
    maximum_future_days: int,
) -> DataFrame:
    field = PARTITION_TIMESTAMP_FIELDS[topic]
    field_exists = field in df.columns
    primary = timestamp_expression(df, field)
    kafka_ts = timestamp_expression(df, "kafka_timestamp")
    ingested = timestamp_expression(df, "ingested_at")

    primary_ok = valid_timestamp(primary, minimum_valid_year, maximum_future_days)
    kafka_ok = valid_timestamp(kafka_ts, minimum_valid_year, maximum_future_days)
    ingested_ok = valid_timestamp(ingested, minimum_valid_year, maximum_future_days)

    chosen = (
        F.when(primary_ok, primary)
        .when(kafka_ok, kafka_ts)
        .when(ingested_ok, ingested)
        .otherwise(F.lit(None).cast("timestamp"))
    )
    source_label = (
        F.when(primary_ok, F.lit(field))
        .when(kafka_ok, F.lit("kafka_timestamp"))
        .when(ingested_ok, F.lit("ingested_at"))
        .otherwise(F.lit("unresolved"))
    )

    if not field_exists:
        quality = F.lit("field_missing")
    else:
        upper = F.current_timestamp() + F.expr(
            f"INTERVAL {int(maximum_future_days)} DAYS"
        )
        quality = (
            F.when(F.col(field).isNull(), F.lit("null"))
            .when(primary.isNull(), F.lit("unparseable"))
            .when(F.year(primary) == 1970, F.lit("epoch_1970"))
            .when(F.year(primary) < minimum_valid_year, F.lit("too_old"))
            .when(primary > upper, F.lit("future"))
            .otherwise(F.lit("valid"))
        )

    result = (
        df.drop("year", "month", "day")
        .withColumn("_bronze_partition_timestamp", chosen)
        .withColumn("_bronze_partition_ts_source", source_label)
        .withColumn("_bronze_business_ts_quality", quality)
        .withColumn("year", F.year("_bronze_partition_timestamp"))
        .withColumn("month", F.month("_bronze_partition_timestamp"))
        .withColumn("day", F.dayofmonth("_bronze_partition_timestamp"))
    )

    unresolved = result.filter(F.col("_bronze_partition_timestamp").isNull()).count()
    if unresolved:
        raise ValueError(
            f"{unresolved} rows have no valid business, Kafka, or ingestion timestamp"
        )
    return result


def canonicalize(
    original: DataFrame,
    recovery: Optional[DataFrame],
    *,
    topic: str,
    minimum_valid_year: int,
    maximum_future_days: int,
) -> tuple[DataFrame, list[str]]:
    combined = original
    if recovery is not None:
        combined = combined.unionByName(recovery, allowMissingColumns=True)
    require_columns(combined, KAFKA_IDENTITY, "combined Bronze")
    combined = add_payload_hash(combined)
    payload_columns = hash_columns(combined)

    window = Window.partitionBy(*KAFKA_IDENTITY).orderBy(
        F.col("_bronze_priority").desc(),
        F.col("ingested_at").desc_nulls_last(),
        F.col("kafka_timestamp").desc_nulls_last(),
        F.col("_bronze_payload_hash").desc(),
    )
    canonical = (
        combined.withColumn("_bronze_rank", F.row_number().over(window))
        .filter(F.col("_bronze_rank") == 1)
        .drop("_bronze_rank")
    )
    return (
        add_safe_partitions(
            canonical,
            topic=topic,
            minimum_valid_year=minimum_valid_year,
            maximum_future_days=maximum_future_days,
        ),
        payload_columns,
    )


def validate_output(
    spark: SparkSession,
    *,
    canonical: DataFrame,
    destination: str,
    payload_columns: list[str],
) -> dict[str, Any]:
    output = spark.read.option("mergeSchema", "true").option("recursiveFileLookup", "true").parquet(destination)
    require_columns(output, KAFKA_IDENTITY, "historical_v1 output")

    canonical_count = canonical.count()
    output_count = output.count()
    duplicate_rows = output_count - output.select(*KAFKA_IDENTITY).distinct().count()

    expected = (
        canonical.select(*KAFKA_IDENTITY, "_bronze_payload_hash")
        .dropDuplicates(KAFKA_IDENTITY)
    )
    actual = (
        add_payload_hash(output, payload_columns)
        .select(*KAFKA_IDENTITY, "_bronze_payload_hash")
        .dropDuplicates(KAFKA_IDENTITY)
    )
    missing = expected.join(
        actual,
        KAFKA_IDENTITY + ["_bronze_payload_hash"],
        "left_anti",
    ).count()
    unexpected = actual.join(
        expected,
        KAFKA_IDENTITY + ["_bronze_payload_hash"],
        "left_anti",
    ).count()
    null_partitions = output.filter(F.col("ingested_at").isNull()).count()

    report = {
        "canonical_count": canonical_count,
        "output_count": output_count,
        "output_duplicate_identity_rows": duplicate_rows,
        "missing_identity_payload_pairs": missing,
        "unexpected_identity_payload_pairs": unexpected,
        "null_partition_rows": null_partitions,
    }
    failed = canonical_count != output_count or any(
        report[key] != 0
        for key in (
            "output_duplicate_identity_rows",
            "missing_identity_payload_pairs",
            "unexpected_identity_payload_pairs",
            "null_partition_rows",
        )
    )
    if failed:
        raise RuntimeError(
            "historical_v1 validation failed; new output retained for inspection; "
            f"existing paths untouched: {report}"
        )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Bronze historical_v1")
    parser.add_argument("--topic", required=True, choices=BUSINESS_TOPICS)
    parser.add_argument("--original-path")
    parser.add_argument("--recovery-path")
    parser.add_argument("--no-recovery", action="store_true")
    parser.add_argument("--output-path")
    parser.add_argument(
        "--output-partitions",
        type=int,
        default=int(os.getenv("BRONZE_MIGRATION_OUTPUT_PARTITIONS", "8")),
    )
    parser.add_argument("--minimum-valid-year", type=int, default=2000)
    parser.add_argument("--maximum-future-days", type=int, default=1)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually write; without it the command is a dry run",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    validate_topic(args.topic)

    source = args.original_path or original_path(args.topic)
    recovery = None if args.no_recovery else (
        args.recovery_path if args.recovery_path is not None else recovery_path(args.topic)
    )
    destination = args.output_path or output_path(args.topic)

    if destination in {source, recovery}:
        raise ValueError("Output path must differ from every source path")

    spark = build_spark(args.topic)
    try:
        if not path_exists(spark, source):
            raise FileNotFoundError(source)
        if path_exists(spark, destination):
            raise FileExistsError(
                f"Output already exists; choose a new versioned path: {destination}"
            )

        original_df = read_source(
            spark,
            path=source,
            generation="original",
            priority=1,
        )
        recovery_df = None
        recovery_exists = bool(recovery and path_exists(spark, recovery))
        if recovery_exists:
            recovery_df = read_source(
                spark,
                path=recovery,
                generation="recovery",
                priority=2,
            )

        canonical, payload_columns = canonicalize(
            original_df,
            recovery_df,
            topic=args.topic,
            minimum_valid_year=args.minimum_valid_year,
            maximum_future_days=args.maximum_future_days,
        )
        canonical = canonical.cache()

        original_count = original_df.count()
        recovery_count = recovery_df.count() if recovery_df is not None else 0
        canonical_count = canonical.count()
        plan = {
            "topic": args.topic,
            "original_path": source,
            "recovery_path": recovery,
            "recovery_exists": recovery_exists,
            "output_path": destination,
            "original_count": original_count,
            "recovery_count": recovery_count,
            "combined_count": original_count + recovery_count,
            "canonical_count": canonical_count,
            "duplicates_removed": original_count + recovery_count - canonical_count,
            "output_partitions": args.output_partitions,
            "write_mode": "errorifexists",
            "execute": args.execute,
        }
        print(json.dumps(plan, indent=2, default=str))

        if not args.execute:
            print("[DRY RUN] No data was written")
            return

        dated = canonical.withColumn("_historical_date", F.to_date("ingested_at"))
        invalid_ingested_at = dated.filter(F.col("_historical_date").isNull()).count()
        if invalid_ingested_at:
            raise ValueError(f"{invalid_ingested_at} rows have no valid ingested_at date")

        dates = (
            dated.select("_historical_date")
            .distinct()
            .orderBy("_historical_date")
            .collect()
        )
        for row in dates:
            ingested_date = row["_historical_date"]
            date_destination = (
                f"{destination}/{ingested_date.year:04d}/"
                f"{ingested_date.month:02d}/{ingested_date.day:02d}"
            )
            (
                dated.filter(F.col("_historical_date") == F.lit(ingested_date))
                .drop("_bronze_payload_hash", "_historical_date")
                .repartition(args.output_partitions)
                .sortWithinPartitions("kafka_partition", "kafka_offset")
                .write.format("parquet")
                .mode("errorifexists")
                .save(date_destination)
            )

        report = validate_output(
            spark,
            canonical=canonical,
            destination=destination,
            payload_columns=payload_columns,
        )
        print(json.dumps(report, indent=2, default=str))
        print("[PASS] historical_v1 written and validated; sources untouched")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
