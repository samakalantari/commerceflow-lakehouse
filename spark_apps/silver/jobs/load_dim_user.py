from spark_apps.silver.common.bronze_reader import (
    read_bronze_topic,
)
from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    DIM_USER,
    INVALID_USERS,
    QUARANTINE_DATABASE,
    TOPIC_USERS,
)
from spark_apps.silver.dimensions.dim_user import (
    build_dim_user_source,
)


def main() -> None:
    spark = build_iceberg_spark(
        "silver-load-dim-user"
    )

    try:
        print("=" * 100)
        print("BUILDING DIM_USER")
        print("=" * 100)

        # -----------------------------------------------------
        # 1. Read Bronze
        # -----------------------------------------------------

        bronze_df = read_bronze_topic(
            spark,
            TOPIC_USERS,
        )

        bronze_count = (
            bronze_df.count()
        )

        print(
            f"Bronze users: "
            f"{bronze_count:,}"
        )

        # -----------------------------------------------------
        # 2. Clean + Validate
        # -----------------------------------------------------

        valid_df, invalid_df = (
            build_dim_user_source(
                bronze_df
            )
        )

        valid_df = valid_df.cache()
        invalid_df = invalid_df.cache()

        valid_count = (
            valid_df.count()
        )

        invalid_count = (
            invalid_df.count()
        )

        print(
            f"Valid users: "
            f"{valid_count:,}"
        )

        print(
            f"Invalid users: "
            f"{invalid_count:,}"
        )

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

        print(
            "[PASS] Unknown DIM_USER member ensured."
        )

        # -----------------------------------------------------
        # 5. Type 1 MERGE
        # -----------------------------------------------------

        valid_df.createOrReplaceTempView(
            "staged_dim_user"
        )

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

        print(
            "[PASS] DIM_USER MERGE completed."
        )

        # -----------------------------------------------------
        # 6. Quarantine invalid users
        # -----------------------------------------------------

        spark.sql(
            f"""
            CREATE NAMESPACE IF NOT EXISTS
            {QUARANTINE_DATABASE}
            """
        )

        if invalid_count > 0:
            (
                invalid_df
                .writeTo(
                    INVALID_USERS
                )
                .using(
                    "iceberg"
                )
                .tableProperty(
                    "format-version",
                    "2",
                )
                .createOrReplace()
            )

            print(
                f"[WARN] {invalid_count:,} "
                "invalid users written "
                "to quarantine."
            )

        # -----------------------------------------------------
        # 7. Final Audit
        # -----------------------------------------------------

        dim_df = spark.table(
            DIM_USER
        )

        silver_count = (
            dim_df.count()
        )

        unknown_count = (
            dim_df
            .filter(
                "user_sk = -1"
            )
            .count()
        )

        print()
        print("DIM_USER AUDIT")
        print("-" * 100)

        print(
            f"DIM_USER records: "
            f"{silver_count:,}"
        )

        print(
            f"Unknown user records: "
            f"{unknown_count:,}"
        )

        if unknown_count != 1:
            raise RuntimeError(
                "DIM_USER Unknown User audit failed."
            )

        print(
            "[PASS] DIM_USER audit completed."
        )

        print("\nDIM_USER SAMPLE")

        (
            dim_df
            .select(
                "user_sk",
                "user_id",
                "username",
                "email",
                "device",
                "loyalty_tier",
                "location",
            )
            .limit(
                5
            )
            .show(
                truncate=False
            )
        )

        print()
        print("=" * 100)
        print(
            "DIM_USER LOAD COMPLETED"
        )
        print("=" * 100)

        valid_df.unpersist()
        invalid_df.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
