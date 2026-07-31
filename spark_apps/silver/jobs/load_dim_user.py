from spark_apps.silver.common.bronze_reader import (
    bronze_topic_day_path,
    read_bronze_topic,
    split_tombstones,
    apply_tombstone_deletes,
)
from spark_apps.silver.common.job_arguments import get_source_selection
from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    DIM_USER,
    FACT_ORDER,
    INVALID_USERS,
    QUARANTINE_DATABASE,
    TOPIC_USERS,
)
from spark_apps.silver.dimensions.dim_user import (
    build_dim_user_source,
)
from spark_apps.silver.quality.quarantine import (
    prepare_quarantine_records,
    write_quarantine,
)


def main() -> None:

    source = get_source_selection()
    spark = build_iceberg_spark("silver-load-dim-user")

    try:
        print("=" * 100)
        print("BUILDING DIM_USER")
        print("=" * 100)

        # -----------------------------------------------------
        # 1. Read Bronze
        # -----------------------------------------------------
        print(f"Bronze source mode: {source.mode}")

        bronze_df = read_bronze_topic(
            spark,
            TOPIC_USERS,
            ingested_date=source.ingested_date,
            source_mode=source.mode,
        ).select(
            "user_id",
            "username",
            "email",
            "signup_date",
            "device",
            "loyalty_tier",
            "location",
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingested_at",
        )
        bronze_df, user_tombstones = split_tombstones(
            bronze_df,
            business_key="user_id",
            payload_columns=("user_id", "username", "email", "signup_date", "device", "loyalty_tier", "location"),
        )

        # -----------------------------------------------------
        # 2. Clean + Validate
        # -----------------------------------------------------

        # Keep the shared normalize/deduplicate/validation result
        # so the valid and invalid actions do not repeat the same
        # Bronze scan, shuffle, and Window sort.
        valid_df, invalid_df = build_dim_user_source(
            bronze_df,
            persist_classified=True,
        )

        valid_df = valid_df.cache()
        invalid_df = invalid_df.cache()

        valid_count = valid_df.count()

        invalid_count = invalid_df.count()

        print(f"Valid users: {valid_count:,}")

        print(f"Invalid users: {invalid_count:,}")

        # -----------------------------------------------------
        # 3. Create DIM_USER Iceberg table
        # -----------------------------------------------------

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS
            {DIM_USER}
            (
                user_sk BIGINT,
                user_id STRING,
                username STRING,
                email STRING,
                signup_date DATE,
                device STRING,
                loyalty_tier STRING,
                location STRING,
                record_hash STRING,
                source_kafka_timestamp TIMESTAMP,
                silver_created_at TIMESTAMP,
                silver_updated_at TIMESTAMP
            )
            USING iceberg
            TBLPROPERTIES (
                'format-version' = '2'
            )
            """
        )

        # -----------------------------------------------------
        # 4. Ensure Unknown User member exists
        #
        # Orders whose user cannot be resolved will use
        # user_sk = -1 instead of NULL.
        # -----------------------------------------------------

        spark.sql(
            f"""
            MERGE INTO
                {DIM_USER} AS target

            USING (
                SELECT
                    CAST(-1 AS BIGINT)
                        AS user_sk,

                    '__UNKNOWN__'
                        AS user_id,

                    'Unknown'
                        AS username,

                    'unknown@unknown.local'
                        AS email,

                    CAST(
                        '1970-01-01'
                        AS DATE
                    )
                        AS signup_date,

                    'Unknown'
                        AS device,

                    'Bronze'
                        AS loyalty_tier,

                    'Unknown'
                        AS location,

                    'UNKNOWN'
                        AS record_hash,

                    CAST(
                        NULL
                        AS TIMESTAMP
                    )
                        AS source_kafka_timestamp
            ) AS source

            ON
                target.user_sk =
                source.user_sk

            WHEN NOT MATCHED THEN

                INSERT (
                    user_sk,
                    user_id,
                    username,
                    email,
                    signup_date,
                    device,
                    loyalty_tier,
                    location,
                    record_hash,
                    source_kafka_timestamp,
                    silver_created_at,
                    silver_updated_at
                )

                VALUES (
                    source.user_sk,
                    source.user_id,
                    source.username,
                    source.email,
                    source.signup_date,
                    source.device,
                    source.loyalty_tier,
                    source.location,
                    source.record_hash,
                    source.source_kafka_timestamp,
                    current_timestamp(),
                    current_timestamp()
                )
            """
        )

        print("[PASS] Unknown DIM_USER member ensured.")

        # -----------------------------------------------------
        # 5. Type 1 MERGE
        # -----------------------------------------------------

        if source.mode == "daily":
            user_tombstones.createOrReplaceTempView("deleted_users")
            spark.sql(
                f"""
                CREATE OR REPLACE TEMP VIEW deleted_user_sks AS
                SELECT DISTINCT dim.user_sk
                FROM {DIM_USER} AS dim
                INNER JOIN deleted_users AS deleted
                    ON dim.user_id = deleted.user_id
                """
            )
            spark.sql(
                f"""
                MERGE INTO {FACT_ORDER} AS target
                USING deleted_user_sks AS source
                ON target.user_sk = source.user_sk
                WHEN MATCHED THEN UPDATE SET
                    target.user_sk = CAST(-1 AS BIGINT),
                    target.silver_updated_at = current_timestamp()
                """
            )
            apply_tombstone_deletes(
                spark, table_name=DIM_USER, business_key="user_id",
                tombstones=user_tombstones, view_name="deleted_users"
            )

        valid_df.createOrReplaceTempView("staged_dim_user")

        spark.sql(
            f"""
            MERGE INTO
                {DIM_USER} AS target

            USING
                staged_dim_user AS source

            ON
                target.user_id =
                source.user_id

            WHEN MATCHED
            AND
                target.record_hash
                <> source.record_hash

            THEN UPDATE SET

                target.username =
                    source.username,

                target.email =
                    source.email,

                target.signup_date =
                    source.signup_date,

                target.device =
                    source.device,

                target.loyalty_tier =
                    source.loyalty_tier,

                target.location =
                    source.location,

                target.record_hash =
                    source.record_hash,

                target.source_kafka_timestamp =
                    source.source_kafka_timestamp,

                target.silver_updated_at =
                    current_timestamp()

            WHEN NOT MATCHED THEN

                INSERT (
                    user_sk,
                    user_id,
                    username,
                    email,
                    signup_date,
                    device,
                    loyalty_tier,
                    location,
                    record_hash,
                    source_kafka_timestamp,
                    silver_created_at,
                    silver_updated_at
                )

                VALUES (
                    source.user_sk,
                    source.user_id,
                    source.username,
                    source.email,
                    source.signup_date,
                    source.device,
                    source.loyalty_tier,
                    source.location,
                    source.record_hash,
                    source.source_kafka_timestamp,
                    current_timestamp(),
                    current_timestamp()
                )
            """
        )

        print("[PASS] DIM_USER MERGE completed.")

        # -----------------------------------------------------
        # 6. Quarantine invalid users
        # -----------------------------------------------------

        spark.sql(
            f"""
            CREATE NAMESPACE IF NOT EXISTS
            {QUARANTINE_DATABASE}
            """
        )

        quarantine_df = prepare_quarantine_records(
            invalid_df,
            entity_name="user",
            source_topic=TOPIC_USERS,
        )
        write_quarantine(quarantine_df, INVALID_USERS)

        print(f"[INFO] Current invalid users in quarantine: {invalid_count:,}")

        # -----------------------------------------------------
        # 7. Final Audit
        # -----------------------------------------------------

        dim_df = spark.table(DIM_USER)

        silver_count = dim_df.count()

        unknown_count = dim_df.filter("user_sk = -1").count()

        print()
        print("DIM_USER AUDIT")
        print("-" * 100)

        print(f"DIM_USER records: {silver_count:,}")

        print(f"Unknown user records: {unknown_count:,}")

        if unknown_count != 1:
            raise RuntimeError("DIM_USER Unknown User audit failed.")

        print("[PASS] DIM_USER audit completed.")

        print("\nDIM_USER SAMPLE")

        (
            dim_df.select(
                "user_sk",
                "user_id",
                "username",
                "email",
                "device",
                "loyalty_tier",
                "location",
            )
            .limit(5)
            .show(truncate=False)
        )

        print()
        print("=" * 100)
        print("DIM_USER LOAD COMPLETED")
        print("=" * 100)

        valid_df.unpersist()
        invalid_df.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
