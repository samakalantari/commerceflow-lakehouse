from pyspark.sql import functions as F

from spark_apps.silver.common.bronze_reader import (
    read_bronze_topic,
)
from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    DIM_USER,
    FACT_ORDER,
    TOPIC_ORDERS,
)
from spark_apps.silver.facts.fact_order import (
    build_fact_order_source,
)


def main() -> None:

    spark = build_iceberg_spark(
        "silver-load-fact-order"
    )

    try:

        print("=" * 100)
        print("BUILDING FACT_ORDER")
        print("=" * 100)

        orders_df = read_bronze_topic(
            spark,
            TOPIC_ORDERS,
        )

        dim_user_df = spark.table(
            DIM_USER
        )

        source_df = (
            build_fact_order_source(
                orders_df,
                dim_user_df,
            )
            .cache()
        )

        source_count = (
            source_df.count()
        )

        missing_user_sk = (
            source_df
            .filter(
                F.col(
                    "user_sk"
                ).isNull()
            )
            .count()
        )

        print(
            f"Source orders: "
            f"{source_count:,}"
        )

        print(
            f"Missing user_sk: "
            f"{missing_user_sk:,}"
        )

        # -----------------------------------------------------
        # Create Iceberg Fact Table
        # -----------------------------------------------------

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS
            {FACT_ORDER}
            (
                order_sk BIGINT,
                order_id STRING,
                user_sk BIGINT,
                order_date_sk INT,
                order_timestamp TIMESTAMP,
                order_total DECIMAL(10,2),
                status STRING,
                payment_method STRING,
                source_kafka_timestamp TIMESTAMP,
                silver_created_at TIMESTAMP,
                silver_updated_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (
                days(order_timestamp)
            )
            TBLPROPERTIES (
                'format-version' = '2'
            )
            """
        )

        source_df.createOrReplaceTempView(
            "staged_fact_order"
        )

        # -----------------------------------------------------
        # Idempotent MERGE
        # -----------------------------------------------------

        spark.sql(
            f"""
            MERGE INTO
                {FACT_ORDER} AS target

            USING
                staged_fact_order AS source

            ON
                target.order_id =
                source.order_id

            WHEN MATCHED
            AND (
                target.user_sk
                    <> source.user_sk

                OR target.order_date_sk
                    <> source.order_date_sk

                OR target.order_timestamp
                    <> source.order_timestamp

                OR target.order_total
                    <> source.order_total

                OR target.status
                    <> source.status

                OR target.payment_method
                    <> source.payment_method
            )

            THEN UPDATE SET

                target.user_sk =
                    source.user_sk,

                target.order_date_sk =
                    source.order_date_sk,

                target.order_timestamp =
                    source.order_timestamp,

                target.order_total =
                    source.order_total,

                target.status =
                    source.status,

                target.payment_method =
                    source.payment_method,

                target.source_kafka_timestamp =
                    source.source_kafka_timestamp,

                target.silver_updated_at =
                    current_timestamp()

            WHEN NOT MATCHED THEN

                INSERT (
                    order_sk,
                    order_id,
                    user_sk,
                    order_date_sk,
                    order_timestamp,
                    order_total,
                    status,
                    payment_method,
                    source_kafka_timestamp,
                    silver_created_at,
                    silver_updated_at
                )

                VALUES (
                    source.order_sk,
                    source.order_id,
                    source.user_sk,
                    source.order_date_sk,
                    source.order_timestamp,
                    source.order_total,
                    source.status,
                    source.payment_method,
                    source.source_kafka_timestamp,
                    current_timestamp(),
                    current_timestamp()
                )
            """
        )

        print(
            "[PASS] FACT_ORDER MERGE completed."
        )

        # -----------------------------------------------------
        # Audit
        # -----------------------------------------------------

        fact_df = spark.table(
            FACT_ORDER
        )

        fact_count = (
            fact_df.count()
        )

        distinct_orders = (
            fact_df
            .select(
                "order_id"
            )
            .distinct()
            .count()
        )

        null_users = (
            fact_df
            .filter(
                F.col(
                    "user_sk"
                ).isNull()
            )
            .count()
        )

        print()
        print("FACT_ORDER AUDIT")
        print("-" * 100)

        print(
            f"Fact rows: "
            f"{fact_count:,}"
        )

        print(
            f"Distinct orders: "
            f"{distinct_orders:,}"
        )

        print(
            f"Null user_sk: "
            f"{null_users:,}"
        )

        print("\nSAMPLE")

        (
            fact_df
            .orderBy(
                F.col(
                    "order_timestamp"
                ).desc()
            )
            .limit(10)
            .show(
                truncate=False
            )
        )

        if (
            fact_count
            ==
            distinct_orders
            and
            null_users
            ==
            0
        ):

            print()
            print(
                "[PASS] FACT_ORDER LOAD COMPLETED"
            )

        else:

            print()
            print(
                "[FAIL] FACT_ORDER AUDIT FAILED"
            )

        source_df.unpersist()

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
