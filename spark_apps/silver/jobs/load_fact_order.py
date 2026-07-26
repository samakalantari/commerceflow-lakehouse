from pyspark.sql import functions as F

from spark_apps.silver.common.bronze_reader import (
    bronze_topic_paths,
    read_bronze_topic,
)
from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    DIM_USER,
    FACT_ORDER,
    INVALID_ORDERS,
    QUARANTINE_DATABASE,
    TOPIC_ORDERS,
)
from spark_apps.silver.facts.fact_order import (
    build_fact_order_source,
)
from spark_apps.silver.quality.quarantine import (
    prepare_quarantine_records,
    write_quarantine,
)


def main() -> None:

    spark = build_iceberg_spark("silver-load-fact-order")

    try:
        print("=" * 100)
        print("BUILDING FACT_ORDER")
        print("=" * 100)

        # -----------------------------------------------------
        # 1. Read source data
        # -----------------------------------------------------

        print("Bronze input paths:")
        for path in bronze_topic_paths(TOPIC_ORDERS):
            print(f"  - {path}/year=*/month=*/day=*")

        orders_df = read_bronze_topic(
            spark,
            TOPIC_ORDERS,
        ).select(
            "order_id", "user_id", "timestamp", "total",
            "status", "payment_method",
            "kafka_timestamp", "kafka_partition", "kafka_offset",
        )

        dim_user_df = spark.table(DIM_USER)

        # -----------------------------------------------------
        # 2. Build canonical source
        # -----------------------------------------------------

        source_df, invalid_df = build_fact_order_source(
            orders_df,
            dim_user_df,
            persist_classified=True,
        )

        source_df = source_df.cache()

        source_count = source_df.count()

        source_distinct_orders = source_df.select("order_id").distinct().count()

        source_duplicate_order_sk = (
            source_df.groupBy("order_sk").count().filter(F.col("count") > 1).count()
        )

        missing_user_sk = source_df.filter(F.col("user_sk").isNull()).count()

        unknown_user_count = source_df.filter(F.col("user_sk") == -1).count()

        invalid_count = invalid_df.count()

        # -----------------------------------------------------
        # 3. Pre-write audit
        # -----------------------------------------------------

        print()
        print("FACT_ORDER SOURCE AUDIT")
        print("-" * 100)

        print(f"Source rows: {source_count:,}")

        print(f"Distinct orders: {source_distinct_orders:,}")

        print(f"Duplicate order_sk: {source_duplicate_order_sk:,}")

        print(f"Null user_sk: {missing_user_sk:,}")

        print(f"Orders mapped to Unknown User: {unknown_user_count:,}")

        print(f"Invalid orders: {invalid_count:,}")

        if (
            source_count != source_distinct_orders
            or source_duplicate_order_sk != 0
            or missing_user_sk != 0
        ):
            raise RuntimeError("FACT_ORDER canonical source audit failed.")

        print("[PASS] FACT_ORDER canonical source audit completed.")

        # -----------------------------------------------------
        # 4. Quarantine invalid source orders
        # -----------------------------------------------------

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {QUARANTINE_DATABASE}")

        if invalid_count > 0:
            quarantine_df = prepare_quarantine_records(
                invalid_df,
                entity_name="order",
                source_topic=TOPIC_ORDERS,
            )
            write_quarantine(quarantine_df, INVALID_ORDERS)
            print(f"[WARN] {invalid_count:,} invalid orders written to quarantine.")

        # -----------------------------------------------------
        # 5. Create Iceberg Fact Table
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

        # -----------------------------------------------------
        # 6. Prepare final rows
        # -----------------------------------------------------

        write_df = (
            source_df.withColumn(
                "order_total",
                F.col("order_total").cast("decimal(10,2)"),
            )
            .withColumn(
                "silver_created_at",
                F.current_timestamp(),
            )
            .withColumn(
                "silver_updated_at",
                F.current_timestamp(),
            )
            .select(
                "order_sk",
                "order_id",
                "user_sk",
                "order_date_sk",
                "order_timestamp",
                "order_total",
                "status",
                "payment_method",
                "source_kafka_timestamp",
                "silver_created_at",
                "silver_updated_at",
            )
        )

        # -----------------------------------------------------
        # 7. Full Iceberg overwrite
        #
        # Replace the existing fact table with the complete
        # canonical source to remove stale or duplicate rows.
        # -----------------------------------------------------

        (write_df.writeTo(FACT_ORDER).overwrite(F.lit(True)))

        print("[PASS] FACT_ORDER FULL OVERWRITE completed.")

        # -----------------------------------------------------
        # 8. Final Audit
        # -----------------------------------------------------

        fact_df = spark.table(FACT_ORDER)

        fact_count = fact_df.count()

        distinct_orders = fact_df.select("order_id").distinct().count()

        null_users = fact_df.filter(F.col("user_sk").isNull()).count()

        duplicate_order_sk = fact_df.groupBy("order_sk").count().filter(F.col("count") > 1).count()

        print()
        print("FACT_ORDER AUDIT")
        print("-" * 100)

        print(f"Fact rows: {fact_count:,}")

        print(f"Distinct orders: {distinct_orders:,}")

        print(f"Duplicate order_sk: {duplicate_order_sk:,}")

        print(f"Null user_sk: {null_users:,}")

        if fact_count == distinct_orders and duplicate_order_sk == 0 and null_users == 0:
            print()
            print("[PASS] FACT_ORDER LOAD COMPLETED")

        else:
            print()
            print("[FAIL] FACT_ORDER AUDIT FAILED")

            raise RuntimeError("FACT_ORDER audit failed.")

        source_df.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
