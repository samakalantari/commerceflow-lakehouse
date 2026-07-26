from pyspark.sql import functions as F

from spark_apps.silver.common.bronze_reader import (
    bronze_topic_paths,
    read_bronze_topic,
)
from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    DIM_PRODUCT,
    FACT_ORDER,
    FACT_ORDER_ITEM,
    INVALID_ORDER_ITEMS,
    QUARANTINE_DATABASE,
    TOPIC_ORDER_ITEMS,
)
from spark_apps.silver.facts.fact_order_item import (
    build_fact_order_item_source,
)
from spark_apps.silver.quality.quarantine import (
    prepare_quarantine_records,
    write_quarantine,
)


def main() -> None:

    spark = build_iceberg_spark("silver-load-fact-order-item")

    try:
        print("=" * 100)
        print("BUILDING FACT_ORDER_ITEM")
        print("=" * 100)

        # -----------------------------------------------------
        # 1. Read source data
        # -----------------------------------------------------

        print("Bronze input paths:")
        for path in bronze_topic_paths(TOPIC_ORDER_ITEMS):
            print(f"  - {path}/year=*/month=*/day=*")

        items_df = read_bronze_topic(
            spark,
            TOPIC_ORDER_ITEMS,
        ).select(
            "order_item_id", "order_id", "product_id",
            "quantity", "unit_price", "item_total_amount",
            "kafka_timestamp", "kafka_partition", "kafka_offset",
        )

        fact_order_df = spark.table(FACT_ORDER)

        dim_product_df = spark.table(DIM_PRODUCT)

        # -----------------------------------------------------
        # 2. Build canonical source
        # -----------------------------------------------------

        source_df, invalid_df = build_fact_order_item_source(
            items_df,
            fact_order_df,
            dim_product_df,
            persist_classified=True,
        )

        source_df = source_df.cache()

        source_count = source_df.count()

        source_distinct_items = source_df.select("order_item_id").distinct().count()

        duplicate_order_item_sk = (
            source_df.groupBy("order_item_sk").count().filter(F.col("count") > 1).count()
        )

        temporal_count = source_df.filter(F.col("product_resolution") == "temporal").count()

        unknown_product_count = source_df.filter(
            F.col("product_resolution") == "unknown_product"
        ).count()

        missing_product_sk = source_df.filter(F.col("product_sk").isNull()).count()

        missing_order_sk = source_df.filter(F.col("order_sk").isNull()).count()

        invalid_count = invalid_df.count()

        # -----------------------------------------------------
        # 3. Pre-write source audit
        # -----------------------------------------------------

        print()
        print("FACT_ORDER_ITEM SOURCE AUDIT")
        print("-" * 100)

        print(f"Source rows: {source_count:,}")

        print(f"Distinct items: {source_distinct_items:,}")

        print(f"Duplicate order_item_sk: {duplicate_order_item_sk:,}")

        print(f"Temporal matches: {temporal_count:,}")

        print(f"Unknown Product mappings: {unknown_product_count:,}")

        print(f"Missing product_sk: {missing_product_sk:,}")

        print(f"Missing order_sk: {missing_order_sk:,}")

        print(f"Invalid order items: {invalid_count:,}")

        if (
            source_count != source_distinct_items
            or duplicate_order_item_sk != 0
            or missing_product_sk != 0
            or missing_order_sk != 0
        ):
            raise RuntimeError("FACT_ORDER_ITEM canonical source audit failed.")

        print("[PASS] FACT_ORDER_ITEM canonical source audit completed.")

        # -----------------------------------------------------
        # 4. Quarantine invalid source order items
        # -----------------------------------------------------

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {QUARANTINE_DATABASE}")

        if invalid_count > 0:
            quarantine_df = prepare_quarantine_records(
                invalid_df,
                entity_name="order_item",
                source_topic=TOPIC_ORDER_ITEMS,
            )
            write_quarantine(quarantine_df, INVALID_ORDER_ITEMS)
            print(f"[WARN] {invalid_count:,} invalid order items written to quarantine.")

        # -----------------------------------------------------
        # 5. Create Iceberg fact table
        # -----------------------------------------------------

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS
            {FACT_ORDER_ITEM}
            (
                order_item_sk BIGINT,
                order_item_id STRING,
                order_sk BIGINT,
                order_id STRING,
                product_sk BIGINT,
                product_id STRING,
                order_date_sk INT,
                order_timestamp TIMESTAMP,
                quantity INT,
                unit_price DECIMAL(10,2),
                item_total_amount DECIMAL(10,2),
                product_resolution STRING,
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
                "unit_price",
                F.col("unit_price").cast("decimal(10,2)"),
            )
            .withColumn(
                "item_total_amount",
                F.col("item_total_amount").cast("decimal(10,2)"),
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
                "order_item_sk",
                "order_item_id",
                "order_sk",
                "order_id",
                "product_sk",
                "product_id",
                "order_date_sk",
                "order_timestamp",
                "quantity",
                "unit_price",
                "item_total_amount",
                "product_resolution",
                "source_kafka_timestamp",
                "silver_created_at",
                "silver_updated_at",
            )
        )

        # -----------------------------------------------------
        # 7. Full Iceberg overwrite
        #
        # Replace stale or duplicated target rows with the
        # complete canonical source.
        # -----------------------------------------------------

        (write_df.writeTo(FACT_ORDER_ITEM).overwrite(F.lit(True)))

        print("[PASS] FACT_ORDER_ITEM FULL OVERWRITE completed.")

        # -----------------------------------------------------
        # 8. Final Audit
        # -----------------------------------------------------

        fact_df = spark.table(FACT_ORDER_ITEM)

        fact_count = fact_df.count()

        distinct_items = fact_df.select("order_item_id").distinct().count()

        duplicate_target_sk = (
            fact_df.groupBy("order_item_sk").count().filter(F.col("count") > 1).count()
        )

        null_products = fact_df.filter(F.col("product_sk").isNull()).count()

        null_orders = fact_df.filter(F.col("order_sk").isNull()).count()

        print()
        print("FACT_ORDER_ITEM AUDIT")
        print("-" * 100)

        print(f"Fact rows: {fact_count:,}")

        print(f"Distinct items: {distinct_items:,}")

        print(f"Duplicate order_item_sk: {duplicate_target_sk:,}")

        print(f"Null product_sk: {null_products:,}")

        print(f"Null order_sk: {null_orders:,}")

        print()
        print("PRODUCT RESOLUTION")

        (fact_df.groupBy("product_resolution").count().show(truncate=False))

        if (
            fact_count == distinct_items
            and duplicate_target_sk == 0
            and null_products == 0
            and null_orders == 0
        ):
            print()
            print("[PASS] FACT_ORDER_ITEM LOAD COMPLETED")

        else:
            print()
            print("[FAIL] FACT_ORDER_ITEM AUDIT FAILED")

            raise RuntimeError("FACT_ORDER_ITEM audit failed.")

        source_df.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
