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
        # Read Bronze
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
        # Clean + Validate
        # -----------------------------------------------------

        valid_df, invalid_df = (
            build_dim_user_source(
                bronze_df
            )
        )

        valid_df = (
            valid_df.cache()
        )

        invalid_df = (
            invalid_df.cache()
        )

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
        # Create DIM_USER Iceberg table
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
        # Type 1 MERGE
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
        # Quarantine
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
        # Final audit
        # -----------------------------------------------------

        silver_count = (
            spark.table(
                DIM_USER
            )
            .count()
        )

        print()
        print(
            f"DIM_USER records: "
            f"{silver_count:,}"
        )

        print("\nDIM_USER SAMPLE")

        (
            spark.table(
                DIM_USER
            )
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
