from pyspark.sql import functions as F

from spark_apps.silver.common.bronze_reader import (
    read_bronze_topic,
)
from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    DIM_PRODUCT,
    FACT_ORDER,
    FACT_ORDER_ITEM,
    TOPIC_ORDER_ITEMS,
)
from spark_apps.silver.facts.fact_order_item import (
    build_fact_order_item_source,
)


def main() -> None:

    spark = build_iceberg_spark(
        "silver-load-fact-order-item"
    )

    try:

        print("=" * 100)
        print("BUILDING FACT_ORDER_ITEM")
        print("=" * 100)

        # -----------------------------------------------------
        # 1. Read source data
        # -----------------------------------------------------

        items_df = read_bronze_topic(
            spark,
            TOPIC_ORDER_ITEMS,
        )

        fact_order_df = spark.table(
            FACT_ORDER
        )

        dim_product_df = spark.table(
            DIM_PRODUCT
        )

        # -----------------------------------------------------
        # 2. Build canonical source
        # -----------------------------------------------------

        source_df = (
            build_fact_order_item_source(
                items_df,
                fact_order_df,
                dim_product_df,
            )
            .cache()
        )

        source_count = (
            source_df.count()
        )

        source_distinct_items = (
            source_df
            .select(
                "order_item_id"
            )
            .distinct()
            .count()
        )

        duplicate_order_item_sk = (
            source_df
            .groupBy(
                "order_item_sk"
            )
            .count()
            .filter(
                F.col(
                    "count"
                ) > 1
            )
            .count()
        )

        temporal_count = (
            source_df
            .filter(
                F.col(
                    "product_resolution"
                )
                ==
                "temporal"
            )
            .count()
        )

        fallback_count = (
            source_df
            .filter(
                F.col(
                    "product_resolution"
                )
                ==
                "earliest_fallback"
            )
            .count()
        )

        missing_product_sk = (
            source_df
            .filter(
                F.col(
                    "product_sk"
                ).isNull()
            )
            .count()
        )

        missing_order_sk = (
            source_df
            .filter(
                F.col(
                    "order_sk"
                ).isNull()
            )
            .count()
        )

        # -----------------------------------------------------
        # 3. Pre-write source audit
        # -----------------------------------------------------

        print()
        print("FACT_ORDER_ITEM SOURCE AUDIT")
        print("-" * 100)

        print(
            f"Source rows: "
            f"{source_count:,}"
        )

        print(
            f"Distinct items: "
            f"{source_distinct_items:,}"
        )

        print(
            f"Duplicate order_item_sk: "
            f"{duplicate_order_item_sk:,}"
        )

        print(
            f"Temporal matches: "
            f"{temporal_count:,}"
        )

        print(
            f"Earliest fallbacks: "
            f"{fallback_count:,}"
        )

        print(
            f"Missing product_sk: "
            f"{missing_product_sk:,}"
        )

        print(
            f"Missing order_sk: "
            f"{missing_order_sk:,}"
        )

        if (
            source_count
            !=
            source_distinct_items
            or
            duplicate_order_item_sk
            !=
            0
            or
            missing_product_sk
            !=
            0
            or
            missing_order_sk
            !=
            0
        ):
            raise RuntimeError(
                "FACT_ORDER_ITEM canonical "
                "source audit failed."
            )

        print(
            "[PASS] FACT_ORDER_ITEM canonical "
            "source audit completed."
        )

        # -----------------------------------------------------
        # 4. Create Iceberg fact table
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
        # 5. Prepare final rows
        # -----------------------------------------------------

        write_df = (
            source_df
            .withColumn(
                "unit_price",
                F.col(
                    "unit_price"
                ).cast(
                    "decimal(10,2)"
                ),
            )
            .withColumn(
                "item_total_amount",
                F.col(
                    "item_total_amount"
                ).cast(
                    "decimal(10,2)"
                ),
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
        # 6. Full Iceberg overwrite
        #
        # Replace stale or duplicated target rows with the
        # complete canonical source.
        # -----------------------------------------------------

        (
            write_df
            .writeTo(
                FACT_ORDER_ITEM
            )
            .overwrite(
                F.lit(
                    True
                )
            )
        )

        print(
            "[PASS] FACT_ORDER_ITEM "
            "FULL OVERWRITE completed."
        )

        # -----------------------------------------------------
        # 7. Final Audit
        # -----------------------------------------------------

        fact_df = spark.table(
            FACT_ORDER_ITEM
        )

        fact_count = (
            fact_df.count()
        )

        distinct_items = (
            fact_df
            .select(
                "order_item_id"
            )
            .distinct()
            .count()
        )

        duplicate_target_sk = (
            fact_df
            .groupBy(
                "order_item_sk"
            )
            .count()
            .filter(
                F.col(
                    "count"
                ) > 1
            )
            .count()
        )

        null_products = (
            fact_df
            .filter(
                F.col(
                    "product_sk"
                ).isNull()
            )
            .count()
        )

        null_orders = (
            fact_df
            .filter(
                F.col(
                    "order_sk"
                ).isNull()
            )
            .count()
        )

        print()
        print("FACT_ORDER_ITEM AUDIT")
        print("-" * 100)

        print(
            f"Fact rows: "
            f"{fact_count:,}"
        )

        print(
            f"Distinct items: "
            f"{distinct_items:,}"
        )

        print(
            f"Duplicate order_item_sk: "
            f"{duplicate_target_sk:,}"
        )

        print(
            f"Null product_sk: "
            f"{null_products:,}"
        )

        print(
            f"Null order_sk: "
            f"{null_orders:,}"
        )

        print()
        print("PRODUCT RESOLUTION")

        (
            fact_df
            .groupBy(
                "product_resolution"
            )
            .count()
            .show(
                truncate=False
            )
        )

        if (
            fact_count
            ==
            distinct_items
            and
            duplicate_target_sk
            ==
            0
            and
            null_products
            ==
            0
            and
            null_orders
            ==
            0
        ):

            print()
            print(
                "[PASS] FACT_ORDER_ITEM "
                "LOAD COMPLETED"
            )

        else:

            print()
            print(
                "[FAIL] FACT_ORDER_ITEM "
                "AUDIT FAILED"
            )

            raise RuntimeError(
                "FACT_ORDER_ITEM audit failed."
            )

        source_df.unpersist()

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
