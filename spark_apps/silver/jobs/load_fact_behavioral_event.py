import argparse
import os
from datetime import datetime, timedelta
from typing import List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_apps.silver.config.iceberg import (
    ICEBERG_CATALOG_NAME,
    build_iceberg_spark,
)


BRONZE_BASE_PATH = os.getenv(
    "BEHAVIORAL_BRONZE_PATH",
    "s3a://commerceflow-lakehouse/bronze/behavioral/events",
)

NAMESPACE = f"{ICEBERG_CATALOG_NAME}.silver"
VALID_TABLE = f"{NAMESPACE}.fact_behavioral_event"
QUARANTINE_TABLE = f"{NAMESPACE}.quarantine_behavioral_event"

ALLOWED_EVENT_TYPES = [
    "page_view",
    "cart_view",
    "product_search",
    "add_to_cart",
    "wishlist_add",
    "checkout_start",
    "remove_from_cart",
    "payment_attempt",
    "order_complete",
    "review_submit",
]

ALLOWED_DEVICES = [
    "mobile",
    "desktop",
    "tablet",
]


def parse_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(
            value.strip().replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Timestamp must look like '2026-07-12 09:00:00'"
        ) from exc


def get_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--start-ts",
        required=True,
        type=parse_datetime,
    )
    parser.add_argument(
        "--end-ts",
        required=True,
        type=parse_datetime,
    )

    args = parser.parse_args()

    if args.start_ts >= args.end_ts:
        parser.error("--start-ts must be before --end-ts")

    return args


def build_partition_paths(
    spark,
    start_ts: datetime,
    end_ts: datetime,
) -> List[str]:
    last_timestamp = end_ts - timedelta(microseconds=1)
    current_date = start_ts.date()
    last_date = last_timestamp.date()

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path_class = spark.sparkContext._jvm.org.apache.hadoop.fs.Path

    existing_paths: List[str] = []
    missing_paths: List[str] = []

    while current_date <= last_date:
        candidate = (
            f"{BRONZE_BASE_PATH}/"
            f"year={current_date.year}/"
            f"month={current_date.month}/"
            f"day={current_date.day}"
        )

        hadoop_path = path_class(candidate)
        file_system = hadoop_path.getFileSystem(hadoop_conf)

        if file_system.exists(hadoop_path):
            existing_paths.append(candidate)
        else:
            missing_paths.append(candidate)

        current_date += timedelta(days=1)

    if missing_paths:
        print("[INFO] Missing Bronze partition(s):")
        for path in missing_paths:
            print(f"  - {path}")

    return existing_paths


def _blank(column_name: str):
    return (
        F.col(column_name).isNull()
        | (F.trim(F.col(column_name).cast("string")) == "")
    )


def prepare_records(
    bronze_df: DataFrame,
    start_ts: datetime,
    end_ts: datetime,
) -> DataFrame:
    """
    Build event_timestamp, validation errors, raw JSON, and processing flags.

    event_timestamp priority:
        1. Bronze timestamp
        2. kafka_timestamp fallback

    ingested_at is retained for lineage only.
    """

    start_value = start_ts.strftime("%Y-%m-%d %H:%M:%S")
    end_value = end_ts.strftime("%Y-%m-%d %H:%M:%S")

    start_literal = F.lit(start_value).cast("timestamp")
    end_literal = F.lit(end_value).cast("timestamp")

    original_columns = bronze_df.columns
    raw_record_json = F.to_json(
        F.struct(*[F.col(name) for name in original_columns])
    )

    prepared = (
        bronze_df
        .withColumn("_raw_record_json", raw_record_json)
        .withColumn(
            "_parsed_application_timestamp",
            F.to_timestamp(F.col("timestamp")),
        )
        .withColumn(
            "event_timestamp",
            F.coalesce(
                F.col("_parsed_application_timestamp"),
                F.col("kafka_timestamp").cast("timestamp"),
            ),
        )
        .withColumn(
            "event_timestamp_source",
            F.when(
                F.col("_parsed_application_timestamp").isNotNull(),
                F.lit("timestamp"),
            ).when(
                F.col("kafka_timestamp").isNotNull(),
                F.lit("kafka_timestamp"),
            ).otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "normalized_event_type",
            F.lower(F.trim(F.col("event_type"))),
        )
        .withColumn(
            "normalized_device",
            F.lower(F.trim(F.col("device"))),
        )
        .withColumn(
            "bronze_partition_date",
            F.make_date(
                F.col("year").cast("int"),
                F.col("month").cast("int"),
                F.col("day").cast("int"),
            ),
        )
        .withColumn(
            "partition_matches_event_date",
            (
                F.col("event_timestamp").isNotNull()
                & (
                    F.to_date(F.col("event_timestamp"))
                    == F.col("bronze_partition_date")
                )
            ),
        )
        .withColumn(
            "event_key",
            F.when(
                F.col("kafka_topic").isNotNull()
                & F.col("kafka_partition").isNotNull()
                & F.col("kafka_offset").isNotNull(),
                F.concat_ws(
                    ":",
                    F.col("kafka_topic"),
                    F.col("kafka_partition").cast("string"),
                    F.col("kafka_offset").cast("string"),
                ),
            ),
        )
        .withColumn(
            "source_file",
            F.input_file_name(),
        )
        .withColumn(
            "_in_requested_interval",
            (
                F.col("event_timestamp").isNotNull()
                & (F.col("event_timestamp") >= start_literal)
                & (F.col("event_timestamp") < end_literal)
            ),
        )
    )

    error_candidates = F.array(
        F.when(
            F.col("event_timestamp").isNull(),
            F.lit("invalid_event_timestamp"),
        ),
        F.when(
            F.col("kafka_topic").isNull(),
            F.lit("missing_kafka_topic"),
        ),
        F.when(
            F.col("kafka_partition").isNull(),
            F.lit("missing_kafka_partition"),
        ),
        F.when(
            F.col("kafka_offset").isNull(),
            F.lit("missing_kafka_offset"),
        ),
        F.when(
            _blank("user_id"),
            F.lit("missing_user_id"),
        ),
        F.when(
            _blank("session_id"),
            F.lit("missing_session_id"),
        ),
        F.when(
            _blank("event_type"),
            F.lit("missing_event_type"),
        ),
        F.when(
            F.col("normalized_event_type").isNotNull()
            & ~F.col("normalized_event_type").isin(ALLOWED_EVENT_TYPES),
            F.lit("unsupported_event_type"),
        ),
        F.when(
            _blank("device"),
            F.lit("missing_device"),
        ),
        F.when(
            F.col("normalized_device").isNotNull()
            & ~F.col("normalized_device").isin(ALLOWED_DEVICES),
            F.lit("unsupported_device"),
        ),
        F.when(
            F.col("event_timestamp").isNotNull()
            & ~F.col("partition_matches_event_date"),
            F.lit("event_date_partition_mismatch"),
        ),
        F.when(
            F.col("event_data.quantity").isNotNull()
            & (F.col("event_data.quantity") < 0),
            F.lit("negative_quantity"),
        ),
        F.when(
            F.col("event_data.cart_total_items").isNotNull()
            & (F.col("event_data.cart_total_items") < 0),
            F.lit("negative_cart_total_items"),
        ),
        F.when(
            F.col("event_data.cart_value").isNotNull()
            & (F.col("event_data.cart_value") < 0),
            F.lit("negative_cart_value"),
        ),
        F.when(
            F.col("event_data.duration_sec").isNotNull()
            & (F.col("event_data.duration_sec") < 0),
            F.lit("negative_duration_sec"),
        ),
        F.when(
            F.col("event_data.http_status").isNotNull()
            & (
                (F.col("event_data.http_status") < 100)
                | (F.col("event_data.http_status") > 599)
            ),
            F.lit("invalid_http_status"),
        ),
        F.when(
            F.col("event_data.results_count").isNotNull()
            & (F.col("event_data.results_count") < 0),
            F.lit("negative_results_count"),
        ),
        F.when(
            F.col("event_data.clicked_position").isNotNull()
            & (F.col("event_data.clicked_position") < 0),
            F.lit("negative_clicked_position"),
        ),
        F.when(
            F.col("event_data.rating").isNotNull()
            & (
                (F.col("event_data.rating") < 1)
                | (F.col("event_data.rating") > 5)
            ),
            F.lit("rating_out_of_range"),
        ),
        F.when(
            F.col("event_data.text_length").isNotNull()
            & (F.col("event_data.text_length") < 0),
            F.lit("negative_text_length"),
        ),
    )

    return (
        prepared
        .withColumn(
            "_validation_error_candidates",
            error_candidates,
        )
        .withColumn(
            "validation_errors",
            F.expr(
                "filter(_validation_error_candidates, x -> x is not null)"
            ),
        )
        .withColumn(
            "is_valid",
            F.size(F.col("validation_errors")) == 0,
        )
        .withColumn(
            "quarantine_key",
            F.coalesce(
                F.col("event_key"),
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.coalesce(F.col("source_file"), F.lit("")),
                        F.coalesce(F.col("_raw_record_json"), F.lit("")),
                    ),
                    256,
                ),
            ),
        )
    )


def split_records(
    prepared_df: DataFrame,
    start_ts: datetime,
    end_ts: datetime,
) -> Tuple[DataFrame, DataFrame]:
    """
    Valid rows are processed only when event_timestamp is in the interval.

    Invalid timestamp and wrong-partition rows are quarantined whenever their
    Bronze partition is read. MERGE keeps quarantine idempotent.
    """

    start_value = start_ts.strftime("%Y-%m-%d %H:%M:%S")
    end_value = end_ts.strftime("%Y-%m-%d %H:%M:%S")

    valid_df = (
        prepared_df
        .filter(
            F.col("_in_requested_interval")
            & F.col("is_valid")
        )
        .select(
            "event_key",
            "user_id",
            "session_id",
            F.col("normalized_event_type").alias("event_type"),
            F.col("normalized_device").alias("device"),
            "event_timestamp",
            F.col("ingested_at").cast("timestamp").alias("ingested_at"),
            F.col("event_data.product_id").alias("product_id"),
            F.col("event_data.quantity").alias("quantity"),
            F.col("event_data.cart_total_items").alias("cart_total_items"),
            F.to_json(F.col("event_data.cart_items")).alias("cart_items_json"),
            F.col("event_data.cart_value").alias("cart_value"),
            F.col("event_data.shipping_method").alias("shipping_method"),
            F.col("event_data.order_id").alias("order_id"),
            F.col("event_data.fulfillment_speed").alias("fulfillment_speed"),
            F.col("event_data.url_path").alias("url_path"),
            F.col("event_data.duration_sec").alias("duration_sec"),
            F.col("event_data.http_status").alias("http_status"),
            F.col("event_data.payment_type").alias("payment_type"),
            F.col("event_data.success").alias("success"),
            F.col("event_data.error_code").alias("error_code"),
            F.col("event_data.query").alias("search_query"),
            F.col("event_data.results_count").alias("results_count"),
            F.col("event_data.clicked_position").alias("clicked_position"),
            F.col("event_data.rating").alias("rating"),
            F.col("event_data.text_length").alias("text_length"),
            F.col("event_data.wishlist_name").alias("wishlist_name"),
            F.to_json(F.col("event_data")).alias("event_data_json"),
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            F.col("kafka_timestamp").cast("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("silver_created_at"),
        )
        .dropDuplicates(["event_key"])
    )

    quarantine_df = (
        prepared_df
        .filter(
            ~F.col("is_valid")
            & (
                F.col("_in_requested_interval")
                | F.col("event_timestamp").isNull()
                | ~F.col("partition_matches_event_date")
            )
        )
        .select(
            "quarantine_key",
            "event_key",
            "validation_errors",
            F.size(F.col("validation_errors")).alias(
                "validation_error_count"
            ),
            F.col("timestamp").cast("string").alias("raw_timestamp"),
            "event_timestamp",
            "event_timestamp_source",
            F.col("kafka_timestamp").cast("timestamp").alias(
                "kafka_timestamp"
            ),
            F.col("ingested_at").cast("timestamp").alias("ingested_at"),
            "bronze_partition_date",
            F.col("year").cast("int").alias("bronze_year"),
            F.col("month").cast("int").alias("bronze_month"),
            F.col("day").cast("int").alias("bronze_day"),
            "user_id",
            "session_id",
            F.col("normalized_event_type").alias("event_type"),
            F.col("normalized_device").alias("device"),
            F.to_json(F.col("event_data")).alias("event_data_json"),
            "kafka_topic",
            F.col("kafka_partition").cast("int").alias("kafka_partition"),
            F.col("kafka_offset").cast("long").alias("kafka_offset"),
            "source_file",
            F.col("_raw_record_json").alias("raw_record_json"),
            F.lit(start_value).cast("timestamp").alias("run_start_ts"),
            F.lit(end_value).cast("timestamp").alias("run_end_ts"),
            F.current_timestamp().alias("first_quarantined_at"),
            F.current_timestamp().alias("last_seen_at"),
        )
        .dropDuplicates(["quarantine_key"])
    )

    return valid_df, quarantine_df


def create_tables(spark) -> None:
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}"
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {VALID_TABLE}
        (
            event_key STRING,
            user_id STRING,
            session_id STRING,
            event_type STRING,
            device STRING,
            event_timestamp TIMESTAMP,
            ingested_at TIMESTAMP,
            product_id STRING,
            quantity INT,
            cart_total_items INT,
            cart_items_json STRING,
            cart_value DOUBLE,
            shipping_method STRING,
            order_id STRING,
            fulfillment_speed STRING,
            url_path STRING,
            duration_sec INT,
            http_status INT,
            payment_type STRING,
            success BOOLEAN,
            error_code STRING,
            search_query STRING,
            results_count INT,
            clicked_position INT,
            rating INT,
            text_length INT,
            wishlist_name STRING,
            event_data_json STRING,
            kafka_topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP,
            silver_created_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(event_timestamp))
        TBLPROPERTIES ('format-version' = '2')
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE}
        (
            quarantine_key STRING,
            event_key STRING,
            validation_errors ARRAY<STRING>,
            validation_error_count INT,
            raw_timestamp STRING,
            event_timestamp TIMESTAMP,
            event_timestamp_source STRING,
            kafka_timestamp TIMESTAMP,
            ingested_at TIMESTAMP,
            bronze_partition_date DATE,
            bronze_year INT,
            bronze_month INT,
            bronze_day INT,
            user_id STRING,
            session_id STRING,
            event_type STRING,
            device STRING,
            event_data_json STRING,
            kafka_topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            source_file STRING,
            raw_record_json STRING,
            run_start_ts TIMESTAMP,
            run_end_ts TIMESTAMP,
            first_quarantined_at TIMESTAMP,
            last_seen_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(first_quarantined_at))
        TBLPROPERTIES ('format-version' = '2')
        """
    )


def merge_valid(spark, valid_df: DataFrame) -> Tuple[int, int, int]:
    valid_df.createOrReplaceTempView("staged_valid_behavioral_events")

    source_count = valid_df.count()

    if source_count == 0:
        return 0, 0, 0

    existing_before = spark.sql(
        f"""
        SELECT COUNT(DISTINCT target.event_key)
        FROM {VALID_TABLE} target
        INNER JOIN staged_valid_behavioral_events source
            ON target.event_key = source.event_key
        """
    ).first()[0]

    spark.sql(
        f"""
        MERGE INTO {VALID_TABLE} target
        USING staged_valid_behavioral_events source
        ON target.event_key = source.event_key
        WHEN NOT MATCHED THEN INSERT (
            event_key,
            user_id,
            session_id,
            event_type,
            device,
            event_timestamp,
            ingested_at,
            product_id,
            quantity,
            cart_total_items,
            cart_items_json,
            cart_value,
            shipping_method,
            order_id,
            fulfillment_speed,
            url_path,
            duration_sec,
            http_status,
            payment_type,
            success,
            error_code,
            search_query,
            results_count,
            clicked_position,
            rating,
            text_length,
            wishlist_name,
            event_data_json,
            kafka_topic,
            kafka_partition,
            kafka_offset,
            kafka_timestamp,
            silver_created_at
        )
        VALUES (
            source.event_key,
            source.user_id,
            source.session_id,
            source.event_type,
            source.device,
            source.event_timestamp,
            source.ingested_at,
            source.product_id,
            source.quantity,
            source.cart_total_items,
            source.cart_items_json,
            source.cart_value,
            source.shipping_method,
            source.order_id,
            source.fulfillment_speed,
            source.url_path,
            source.duration_sec,
            source.http_status,
            source.payment_type,
            source.success,
            source.error_code,
            source.search_query,
            source.results_count,
            source.clicked_position,
            source.rating,
            source.text_length,
            source.wishlist_name,
            source.event_data_json,
            source.kafka_topic,
            source.kafka_partition,
            source.kafka_offset,
            source.kafka_timestamp,
            source.silver_created_at
        )
        """
    )

    existing_after = spark.sql(
        f"""
        SELECT COUNT(DISTINCT target.event_key)
        FROM {VALID_TABLE} target
        INNER JOIN staged_valid_behavioral_events source
            ON target.event_key = source.event_key
        """
    ).first()[0]

    return (
        source_count,
        existing_before,
        existing_after - existing_before,
    )


def merge_quarantine(
    spark,
    quarantine_df: DataFrame,
) -> Tuple[int, int, int]:
    quarantine_df.createOrReplaceTempView(
        "staged_quarantine_behavioral_events"
    )

    source_count = quarantine_df.count()

    if source_count == 0:
        return 0, 0, 0

    existing_before = spark.sql(
        f"""
        SELECT COUNT(DISTINCT target.quarantine_key)
        FROM {QUARANTINE_TABLE} target
        INNER JOIN staged_quarantine_behavioral_events source
            ON target.quarantine_key = source.quarantine_key
        """
    ).first()[0]

    spark.sql(
        f"""
        MERGE INTO {QUARANTINE_TABLE} target
        USING staged_quarantine_behavioral_events source
        ON target.quarantine_key = source.quarantine_key

        WHEN MATCHED THEN UPDATE SET
            target.event_key = source.event_key,
            target.validation_errors = source.validation_errors,
            target.validation_error_count =
                source.validation_error_count,
            target.raw_timestamp = source.raw_timestamp,
            target.event_timestamp = source.event_timestamp,
            target.event_timestamp_source =
                source.event_timestamp_source,
            target.kafka_timestamp = source.kafka_timestamp,
            target.ingested_at = source.ingested_at,
            target.bronze_partition_date =
                source.bronze_partition_date,
            target.bronze_year = source.bronze_year,
            target.bronze_month = source.bronze_month,
            target.bronze_day = source.bronze_day,
            target.user_id = source.user_id,
            target.session_id = source.session_id,
            target.event_type = source.event_type,
            target.device = source.device,
            target.event_data_json = source.event_data_json,
            target.kafka_topic = source.kafka_topic,
            target.kafka_partition = source.kafka_partition,
            target.kafka_offset = source.kafka_offset,
            target.source_file = source.source_file,
            target.raw_record_json = source.raw_record_json,
            target.run_start_ts = source.run_start_ts,
            target.run_end_ts = source.run_end_ts,
            target.last_seen_at = current_timestamp()

        WHEN NOT MATCHED THEN INSERT (
            quarantine_key,
            event_key,
            validation_errors,
            validation_error_count,
            raw_timestamp,
            event_timestamp,
            event_timestamp_source,
            kafka_timestamp,
            ingested_at,
            bronze_partition_date,
            bronze_year,
            bronze_month,
            bronze_day,
            user_id,
            session_id,
            event_type,
            device,
            event_data_json,
            kafka_topic,
            kafka_partition,
            kafka_offset,
            source_file,
            raw_record_json,
            run_start_ts,
            run_end_ts,
            first_quarantined_at,
            last_seen_at
        )
        VALUES (
            source.quarantine_key,
            source.event_key,
            source.validation_errors,
            source.validation_error_count,
            source.raw_timestamp,
            source.event_timestamp,
            source.event_timestamp_source,
            source.kafka_timestamp,
            source.ingested_at,
            source.bronze_partition_date,
            source.bronze_year,
            source.bronze_month,
            source.bronze_day,
            source.user_id,
            source.session_id,
            source.event_type,
            source.device,
            source.event_data_json,
            source.kafka_topic,
            source.kafka_partition,
            source.kafka_offset,
            source.source_file,
            source.raw_record_json,
            source.run_start_ts,
            source.run_end_ts,
            source.first_quarantined_at,
            source.last_seen_at
        )
        """
    )

    existing_after = spark.sql(
        f"""
        SELECT COUNT(DISTINCT target.quarantine_key)
        FROM {QUARANTINE_TABLE} target
        INNER JOIN staged_quarantine_behavioral_events source
            ON target.quarantine_key = source.quarantine_key
        """
    ).first()[0]

    return (
        source_count,
        existing_before,
        existing_after - existing_before,
    )



def remove_quarantined_from_valid(
    spark,
    quarantine_source_count: int,
) -> int:
    """
    Remove historical rows from the valid table when the same event_key is
    now classified as invalid. Rows without event_key could never have been
    inserted into the valid table.
    """

    if quarantine_source_count == 0:
        return 0

    rows_to_remove = spark.sql(
        f"""
        SELECT COUNT(DISTINCT target.event_key)
        FROM {VALID_TABLE} target
        INNER JOIN staged_quarantine_behavioral_events source
            ON target.event_key = source.event_key
        WHERE source.event_key IS NOT NULL
        """
    ).first()[0]

    if rows_to_remove == 0:
        return 0

    spark.sql(
        f"""
        MERGE INTO {VALID_TABLE} target
        USING (
            SELECT DISTINCT event_key
            FROM staged_quarantine_behavioral_events
            WHERE event_key IS NOT NULL
        ) source
        ON target.event_key = source.event_key
        WHEN MATCHED THEN DELETE
        """
    )

    return rows_to_remove

def show_validation_summary(
    quarantine_df: DataFrame,
) -> None:
    print()
    print("=" * 100)
    print("VALIDATION ERROR SUMMARY")
    print("=" * 100)

    (
        quarantine_df
        .select(
            F.explode("validation_errors").alias("validation_error")
        )
        .groupBy("validation_error")
        .count()
        .orderBy(F.col("count").desc())
        .show(100, truncate=False)
    )


def main() -> None:
    args = get_arguments()

    spark = build_iceberg_spark(
        "silver-load-fact-behavioral-event-with-quarantine"
    )

    prepared_df = None
    valid_df = None
    quarantine_df = None

    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        spark.sparkContext.setLogLevel("WARN")

        print("=" * 100)
        print("BEHAVIORAL SILVER LOAD WITH QUARANTINE")
        print("=" * 100)
        print("Valid table:", VALID_TABLE)
        print("Quarantine table:", QUARANTINE_TABLE)
        print("Start:", args.start_ts)
        print("End:", args.end_ts)
        print("Event-time priority: timestamp -> kafka_timestamp")
        print("ingested_at: lineage only")

        create_tables(spark)

        partition_paths = build_partition_paths(
            spark,
            args.start_ts,
            args.end_ts,
        )

        print("Bronze paths:")
        for path in partition_paths:
            print(f"  - {path}")

        if not partition_paths:
            print("[PASS] No Bronze partitions found.")
            return

        bronze_df = (
            spark.read
            .option("basePath", BRONZE_BASE_PATH)
            .parquet(*partition_paths)
        )

        prepared_df = prepare_records(
            bronze_df,
            args.start_ts,
            args.end_ts,
        ).cache()

        valid_df, quarantine_df = split_records(
            prepared_df,
            args.start_ts,
            args.end_ts,
        )

        valid_df = valid_df.cache()
        quarantine_df = quarantine_df.cache()

        quarantine_result = merge_quarantine(
            spark,
            quarantine_df,
        )

        removed_from_valid = remove_quarantined_from_valid(
            spark,
            quarantine_result[0],
        )

        valid_result = merge_valid(spark, valid_df)

        print()
        print("=" * 100)
        print("LOAD RESULT")
        print("=" * 100)
        print(f"Valid source rows: {valid_result[0]:,}")
        print(f"Valid rows already in Silver: {valid_result[1]:,}")
        print(f"New valid rows inserted: {valid_result[2]:,}")
        print(f"Invalid source rows: {quarantine_result[0]:,}")
        print(
            "Invalid rows already in quarantine: "
            f"{quarantine_result[1]:,}"
        )
        print(
            "New quarantine rows inserted: "
            f"{quarantine_result[2]:,}"
        )
        print(
            "Historical invalid rows removed from valid table: "
            f"{removed_from_valid:,}"
        )

        if quarantine_result[0] > 0:
            show_validation_summary(quarantine_df)

        duplicate_count = spark.sql(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT event_key
                FROM {VALID_TABLE}
                GROUP BY event_key
                HAVING COUNT(*) > 1
            ) duplicates
            """
        ).first()[0]

        if duplicate_count != 0:
            raise RuntimeError(
                f"Duplicate event keys detected: {duplicate_count}"
            )

        print("[PASS] BEHAVIORAL SILVER LOAD COMPLETED")

    finally:
        if quarantine_df is not None:
            quarantine_df.unpersist()
        if valid_df is not None:
            valid_df.unpersist()
        if prepared_df is not None:
            prepared_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
