import argparse
import os
from datetime import datetime, timedelta
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_apps.silver.config.iceberg import (
    ICEBERG_CATALOG_NAME,
    build_iceberg_spark,
)


BRONZE_BASE_PATH = os.environ[
    "BEHAVIORAL_BRONZE_PATH"
]

NAMESPACE = (
    f"{ICEBERG_CATALOG_NAME}.silver"
)

TABLE = (
    f"{NAMESPACE}.fact_behavioral_event"
)


def parse_datetime(value: str) -> datetime:
    """
    Parse timestamps supplied by Airflow or spark-submit.

    Accepted examples:

        2026-07-12 09:00:00
        2026-07-12T09:00:00
        2026-07-12T09:00:00Z
    """

    try:
        return datetime.fromisoformat(
            value.strip().replace(
                "Z",
                "+00:00",
            )
        )

    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Timestamp must look like "
            "'2026-07-12 09:00:00'"
        ) from exc


def get_arguments() -> argparse.Namespace:
    """
    Read the event-time interval.

    start_ts is inclusive.
    end_ts is exclusive.
    """

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
        parser.error(
            "--start-ts must be before --end-ts"
        )

    return args


def build_partition_paths(
    spark,
    start_ts: datetime,
    end_ts: datetime,
) -> List[str]:
    """
    Return existing daily Bronze folders touched by the
    requested event-time interval.

    Directory layout:

        <BEHAVIORAL_BRONZE_PATH>/YYYY/MM/DD

    Example:

        s3a://commerceflow-lakehouse/bronze_v2/
        behavioral/events/historical_v1/2026/07/23

    Folder selection is based only on the Airflow interval
    date. Timestamp columns are checked after files are read.
    """

    # end_ts is exclusive. An interval ending exactly at
    # midnight must not include the following day.
    last_timestamp = end_ts - timedelta(microseconds=1)

    current_date = start_ts.date()
    last_date = last_timestamp.date()

    hadoop_conf = (
        spark.sparkContext
        ._jsc
        .hadoopConfiguration()
    )

    path_class = (
        spark.sparkContext
        ._jvm
        .org.apache.hadoop.fs.Path
    )

    existing_paths: List[str] = []
    missing_paths: List[str] = []

    while current_date <= last_date:
        candidate = (
            f"{BRONZE_BASE_PATH}/"
            f"{current_date.year:04d}/"
            f"{current_date.month:02d}/"
            f"{current_date.day:02d}"
        )

        hadoop_path = path_class(candidate)
        file_system = hadoop_path.getFileSystem(hadoop_conf)

        if file_system.exists(hadoop_path):
            existing_paths.append(candidate)
        else:
            missing_paths.append(candidate)

        current_date += timedelta(days=1)

    if missing_paths:
        print()
        print("[INFO] Missing Bronze daily folders:")
        for path in missing_paths:
            print(f"  - {path}")

    return existing_paths

def build_source(
    bronze_df: DataFrame,
    start_ts: datetime,
    end_ts: datetime,
) -> DataFrame:
    """
    Convert Bronze behavioral events into the Silver
    fact-table structure.

    Event-time priority:

        1. Bronze timestamp
        2. kafka_timestamp only when timestamp cannot
           be parsed or is null

    ingested_at is not used for filtering.
    It is retained only for lineage and auditing.
    """

    start_value = start_ts.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    end_value = end_ts.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    start_literal = (
        F.lit(start_value)
        .cast("timestamp")
    )

    end_literal = (
        F.lit(end_value)
        .cast("timestamp")
    )

    parsed_application_timestamp = (
        F.to_timestamp(
            F.col("timestamp")
        )
    )

    return (
        bronze_df

        # Save the physical file path so the YYYY/MM/DD
        # folder date can be audited.
        .withColumn(
            "_source_file",
            F.input_file_name(),
        )

        # ---------------------------------------------
        # Step 1: Parse the application's event time.
        # ---------------------------------------------
        .withColumn(
            "_parsed_application_timestamp",
            parsed_application_timestamp,
        )

        # ---------------------------------------------
        # Step 2: Determine the final event timestamp.
        #
        # First choice:
        #     timestamp
        #
        # Fallback:
        #     kafka_timestamp
        # ---------------------------------------------
        .withColumn(
            "event_timestamp",
            F.coalesce(
                F.col(
                    "_parsed_application_timestamp"
                ),
                F.col(
                    "kafka_timestamp"
                ).cast(
                    "timestamp"
                ),
            ),
        )

        # Record which Bronze timestamp was used.
        #
        # This column is used for logging/auditing.
        # It is not inserted into the current target table.
        .withColumn(
            "event_timestamp_source",
            F.when(
                F.col(
                    "_parsed_application_timestamp"
                ).isNotNull(),
                F.lit(
                    "timestamp"
                ),
            ).otherwise(
                F.lit(
                    "kafka_timestamp"
                )
            ),
        )

        # Reconstruct the physical Bronze folder date
        # from paths such as /2026/07/23/file.parquet.
        .withColumn(
            "_partition_year",
            F.regexp_extract(
                F.col("_source_file"),
                r"/(\d{4})/(\d{2})/(\d{2})(?:/|$)",
                1,
            ).cast("int"),
        )
        .withColumn(
            "_partition_month",
            F.regexp_extract(
                F.col("_source_file"),
                r"/(\d{4})/(\d{2})/(\d{2})(?:/|$)",
                2,
            ).cast("int"),
        )
        .withColumn(
            "_partition_day",
            F.regexp_extract(
                F.col("_source_file"),
                r"/(\d{4})/(\d{2})/(\d{2})(?:/|$)",
                3,
            ).cast("int"),
        )
        .withColumn(
            "bronze_partition_date",
            F.make_date(
                F.col("_partition_year"),
                F.col("_partition_month"),
                F.col("_partition_day"),
            ),
        )

        # Check whether the physical Bronze partition
        # matches the calculated event date.
        .withColumn(
            "partition_matches_event_date",
            F.to_date(
                F.col(
                    "event_timestamp"
                )
            ).eqNullSafe(
                F.col(
                    "bronze_partition_date"
                )
            ),
        )

        # ---------------------------------------------
        # Step 3: Remove rows for which neither timestamp
        # nor kafka_timestamp provides a valid time.
        # ---------------------------------------------
        .filter(
            F.col(
                "event_timestamp"
            ).isNotNull()
        )

        # ---------------------------------------------
        # Step 4: Filter entirely by event_timestamp.
        #
        # ingested_at is NOT checked here.
        #
        # start_ts is inclusive.
        # end_ts is exclusive.
        # ---------------------------------------------
        .filter(
            (
                F.col(
                    "event_timestamp"
                )
                >=
                start_literal
            )
            &
            (
                F.col(
                    "event_timestamp"
                )
                <
                end_literal
            )
        )

        # Kafka identity is required to create a stable,
        # unique key for each event.
        .filter(
            F.col(
                "kafka_topic"
            ).isNotNull()
            &
            F.col(
                "kafka_partition"
            ).isNotNull()
            &
            F.col(
                "kafka_offset"
            ).isNotNull()
        )

        # Unique event identity.
        .withColumn(
            "event_key",
            F.concat_ws(
                ":",
                F.col(
                    "kafka_topic"
                ),
                F.col(
                    "kafka_partition"
                ).cast(
                    "string"
                ),
                F.col(
                    "kafka_offset"
                ).cast(
                    "string"
                ),
            ),
        )

        .withColumn(
            "event_type",
            F.lower(
                F.trim(
                    F.col(
                        "event_type"
                    )
                )
            ),
        )

        .withColumn(
            "device",
            F.lower(
                F.trim(
                    F.col(
                        "device"
                    )
                )
            ),
        )

        .select(
            "event_key",
            "user_id",
            "session_id",
            "event_type",
            "device",

            "event_timestamp",

            # ingested_at is saved only for lineage.
            F.col(
                "ingested_at"
            ).cast(
                "timestamp"
            ).alias(
                "ingested_at"
            ),

            F.col(
                "event_data.product_id"
            ).alias(
                "product_id"
            ),

            F.col(
                "event_data.quantity"
            ).alias(
                "quantity"
            ),

            F.col(
                "event_data.cart_total_items"
            ).alias(
                "cart_total_items"
            ),

            F.to_json(
                F.col(
                    "event_data.cart_items"
                )
            ).alias(
                "cart_items_json"
            ),

            F.col(
                "event_data.cart_value"
            ).alias(
                "cart_value"
            ),

            F.col(
                "event_data.shipping_method"
            ).alias(
                "shipping_method"
            ),

            F.col(
                "event_data.order_id"
            ).alias(
                "order_id"
            ),

            F.col(
                "event_data.fulfillment_speed"
            ).alias(
                "fulfillment_speed"
            ),

            F.col(
                "event_data.url_path"
            ).alias(
                "url_path"
            ),

            F.col(
                "event_data.duration_sec"
            ).alias(
                "duration_sec"
            ),

            F.col(
                "event_data.http_status"
            ).alias(
                "http_status"
            ),

            F.col(
                "event_data.payment_type"
            ).alias(
                "payment_type"
            ),

            F.col(
                "event_data.success"
            ).alias(
                "success"
            ),

            F.col(
                "event_data.error_code"
            ).alias(
                "error_code"
            ),

            F.col(
                "event_data.query"
            ).alias(
                "search_query"
            ),

            F.col(
                "event_data.results_count"
            ).alias(
                "results_count"
            ),

            F.col(
                "event_data.clicked_position"
            ).alias(
                "clicked_position"
            ),

            F.col(
                "event_data.rating"
            ).alias(
                "rating"
            ),

            F.col(
                "event_data.text_length"
            ).alias(
                "text_length"
            ),

            F.col(
                "event_data.wishlist_name"
            ).alias(
                "wishlist_name"
            ),

            F.to_json(
                F.col(
                    "event_data"
                )
            ).alias(
                "event_data_json"
            ),

            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",

            F.current_timestamp().alias(
                "silver_created_at"
            ),

            # Temporary audit fields.
            # MERGE does not insert these fields.
            "event_timestamp_source",
            "bronze_partition_date",
            "partition_matches_event_date",
        )

        # Remove duplicate Kafka events appearing inside
        # the same Spark batch.
        .dropDuplicates(
            [
                "event_key"
            ]
        )
    )


def create_table(
    spark,
) -> None:
    """
    Create the Iceberg namespace and target table when
    they do not already exist.

    The existing table already uses the correct
    event_timestamp-day partition.
    """

    spark.sql(
        f"""
        CREATE NAMESPACE IF NOT EXISTS
        {NAMESPACE}
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
        {TABLE}
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

        PARTITIONED BY (
            days(event_timestamp)
        )

        TBLPROPERTIES (
            'format-version' = '2'
        )
        """
    )


def get_source_audit(
    source_df: DataFrame,
):
    """
    Calculate event-time audit values from the cached
    source dataframe.
    """

    return (
        source_df
        .agg(
            F.count(
                "*"
            ).alias(
                "source_rows"
            ),

            F.coalesce(
                F.sum(
                    F.when(
                        F.col(
                            "event_timestamp_source"
                        )
                        ==
                        F.lit(
                            "timestamp"
                        ),
                        1,
                    ).otherwise(
                        0
                    )
                ),
                F.lit(
                    0
                ),
            ).alias(
                "timestamp_rows"
            ),

            F.coalesce(
                F.sum(
                    F.when(
                        F.col(
                            "event_timestamp_source"
                        )
                        ==
                        F.lit(
                            "kafka_timestamp"
                        ),
                        1,
                    ).otherwise(
                        0
                    )
                ),
                F.lit(
                    0
                ),
            ).alias(
                "kafka_fallback_rows"
            ),

            F.coalesce(
                F.sum(
                    F.when(
                        ~F.col(
                            "partition_matches_event_date"
                        ),
                        1,
                    ).otherwise(
                        0
                    )
                ),
                F.lit(
                    0
                ),
            ).alias(
                "partition_mismatch_rows"
            ),

            F.min(
                "event_timestamp"
            ).alias(
                "minimum_event_timestamp"
            ),

            F.max(
                "event_timestamp"
            ).alias(
                "maximum_event_timestamp"
            ),

            F.min(
                "ingested_at"
            ).alias(
                "minimum_ingested_at"
            ),

            F.max(
                "ingested_at"
            ).alias(
                "maximum_ingested_at"
            ),
        )
        .first()
    )


def main() -> None:
    """
    Behavioral Bronze-to-Silver event-time ETL.
    """

    args = get_arguments()

    spark = build_iceberg_spark(
        "silver-load-fact-behavioral-event"
    )

    source_df = None

    try:
        spark.conf.set(
            "spark.sql.session.timeZone",
            "UTC",
        )

        spark.sparkContext.setLogLevel(
            "WARN"
        )

        print()
        print(
            "=" * 100
        )
        print(
            "BEHAVIORAL SILVER EVENT-TIME LOAD"
        )
        print(
            "=" * 100
        )

        print(
            "Table:",
            TABLE,
        )

        print(
            "Event-time start:",
            args.start_ts,
        )

        print(
            "Event-time end:",
            args.end_ts,
        )

        print(
            "Timestamp priority:",
            "timestamp -> kafka_timestamp",
        )

        print(
            "ingested_at usage:",
            "audit/lineage only",
        )

        create_table(
            spark
        )

        partition_paths = (
            build_partition_paths(
                spark,
                args.start_ts,
                args.end_ts,
            )
        )

        print()
        print(
            "Existing Bronze event-date paths:"
        )

        for path in partition_paths:
            print(
                f"  - {path}"
            )

        if not partition_paths:

            print()
            print(
                "[INFO] No Bronze event-date "
                "partitions exist for this interval."
            )

            print(
                "[PASS] Completed with 0 rows."
            )

            return

        # Read Parquet files from the selected daily
        # folders, including any nested subdirectories.
        bronze_df = (
            spark.read
            .option(
                "recursiveFileLookup",
                "true",
            )
            .parquet(
                *partition_paths
            )
        )

        source_df = (
            build_source(
                bronze_df,
                args.start_ts,
                args.end_ts,
            )
            .cache()
        )

        audit = get_source_audit(
            source_df
        )

        source_count = int(
            audit[
                "source_rows"
            ]
        )

        print()
        print(
            "=" * 100
        )
        print(
            "EVENT-TIME SOURCE AUDIT"
        )
        print(
            "=" * 100
        )

        print(
            "Source rows:",
            f"{source_count:,}",
        )

        print(
            "Rows using Bronze timestamp:",
            f"{int(audit['timestamp_rows']):,}",
        )

        print(
            "Rows using kafka_timestamp fallback:",
            f"{int(audit['kafka_fallback_rows']):,}",
        )

        print(
            "Rows whose event date does not match "
            "the Bronze folder date:",
            f"{int(audit['partition_mismatch_rows']):,}",
        )

        print(
            "Minimum event_timestamp:",
            audit[
                "minimum_event_timestamp"
            ],
        )

        print(
            "Maximum event_timestamp:",
            audit[
                "maximum_event_timestamp"
            ],
        )

        print(
            "Minimum ingested_at, for audit only:",
            audit[
                "minimum_ingested_at"
            ],
        )

        print(
            "Maximum ingested_at, for audit only:",
            audit[
                "maximum_ingested_at"
            ],
        )

        if source_count == 0:

            print()
            print(
                "[INFO] The Bronze partition exists, "
                "but contains no events in the requested "
                "event-time interval."
            )

            print(
                "[PASS] Completed with 0 rows."
            )

            return

        source_df.createOrReplaceTempView(
            "staged_behavioral_events"
        )

        (
            source_df
            .select(
                "event_key"
            )
            .createOrReplaceTempView(
                "staged_behavioral_keys"
            )
        )

        existing_before = (
            spark.sql(
                f"""
                SELECT
                    COUNT(
                        DISTINCT target.event_key
                    )

                FROM
                    {TABLE} AS target

                INNER JOIN
                    staged_behavioral_keys
                    AS source

                ON
                    target.event_key =
                    source.event_key
                """
            )
            .first()[0]
        )

        # Idempotent load:
        # existing event keys are not inserted again.
        spark.sql(
            f"""
            MERGE INTO
                {TABLE} AS target

            USING
                staged_behavioral_events
                AS source

            ON
                target.event_key =
                source.event_key

            WHEN NOT MATCHED THEN
                INSERT (
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

        existing_after = (
            spark.sql(
                f"""
                SELECT
                    COUNT(
                        DISTINCT target.event_key
                    )

                FROM
                    {TABLE} AS target

                INNER JOIN
                    staged_behavioral_keys
                    AS source

                ON
                    target.event_key =
                    source.event_key
                """
            )
            .first()[0]
        )

        inserted_count = (
            existing_after
            -
            existing_before
        )

        duplicate_count = (
            spark.sql(
                f"""
                SELECT
                    COUNT(*)

                FROM (
                    SELECT
                        target.event_key

                    FROM
                        {TABLE} AS target

                    INNER JOIN
                        staged_behavioral_keys
                        AS source

                    ON
                        target.event_key =
                        source.event_key

                    GROUP BY
                        target.event_key

                    HAVING
                        COUNT(*) > 1
                ) duplicate_keys
                """
            )
            .first()[0]
        )

        print()
        print(
            "=" * 100
        )
        print(
            "LOAD RESULT"
        )
        print(
            "=" * 100
        )

        print(
            "Source event-time rows:",
            f"{source_count:,}",
        )

        print(
            "Already existed in Silver:",
            f"{existing_before:,}",
        )

        print(
            "New rows inserted:",
            f"{inserted_count:,}",
        )

        print(
            "Source keys now in Silver:",
            f"{existing_after:,}",
        )

        print(
            "Duplicate event keys:",
            f"{duplicate_count:,}",
        )

        print()
        print(
            "SAMPLE LOADED EVENTS"
        )

        spark.sql(
            f"""
            SELECT
                target.event_key,
                target.user_id,
                target.event_type,
                target.event_timestamp,
                target.kafka_timestamp,
                target.ingested_at

            FROM
                {TABLE} AS target

            INNER JOIN
                staged_behavioral_keys
                AS source

            ON
                target.event_key =
                source.event_key

            ORDER BY
                target.event_timestamp

            LIMIT 20
            """
        ).show(
            truncate=False
        )

        if duplicate_count != 0:
            raise RuntimeError(
                "Duplicate event keys were detected "
                "in the Silver table."
            )

        print()
        print(
            "[PASS] BEHAVIORAL SILVER "
            "EVENT-TIME LOAD COMPLETED"
        )

    finally:

        if source_df is not None:
            source_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()