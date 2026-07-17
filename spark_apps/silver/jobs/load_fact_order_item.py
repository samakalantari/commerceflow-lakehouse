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

        print(
            f"Source order items: "
            f"{source_count:,}"
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

        # -----------------------------------------------------
        # Create Iceberg fact table
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

        source_df.createOrReplaceTempView(
            "staged_fact_order_item"
        )

        # -----------------------------------------------------
        # Idempotent MERGE
        # -----------------------------------------------------

        spark.sql(
            f"""
            MERGE INTO
                {FACT_ORDER_ITEM} AS target

            USING
                staged_fact_order_item AS source

            ON
                target.order_item_id =
                source.order_item_id

            WHEN MATCHED
            AND (
                target.order_sk
                    <> source.order_sk

                OR target.product_sk
                    <> source.product_sk

                OR target.order_date_sk
                    <> source.order_date_sk

                OR target.order_timestamp
                    <> source.order_timestamp

                OR target.quantity
                    <> source.quantity

                OR target.unit_price
                    <> source.unit_price

                OR target.item_total_amount
                    <> source.item_total_amount

                OR target.product_resolution
                    <> source.product_resolution
            )

            THEN UPDATE SET

                target.order_sk =
                    source.order_sk,

                target.order_id =
                    source.order_id,

                target.product_sk =
                    source.product_sk,

                target.product_id =
                    source.product_id,

                target.order_date_sk =
                    source.order_date_sk,

                target.order_timestamp =
                    source.order_timestamp,

                target.quantity =
                    source.quantity,

                target.unit_price =
                    source.unit_price,

                target.item_total_amount =
                    source.item_total_amount,

                target.product_resolution =
                    source.product_resolution,

                target.source_kafka_timestamp =
                    source.source_kafka_timestamp,

                target.silver_updated_at =
                    current_timestamp()

            WHEN NOT MATCHED THEN

                INSERT (
                    order_item_sk,
                    order_item_id,
                    order_sk,
                    order_id,
                    product_sk,
                    product_id,
                    order_date_sk,
                    order_timestamp,
                    quantity,
                    unit_price,
                    item_total_amount,
                    product_resolution,
                    source_kafka_timestamp,
                    silver_created_at,
                    silver_updated_at
                )

                VALUES (
                    source.order_item_sk,
                    source.order_item_id,
                    source.order_sk,
                    source.order_id,
                    source.product_sk,
                    source.product_id,
                    source.order_date_sk,
                    source.order_timestamp,
                    source.quantity,
                    source.unit_price,
                    source.item_total_amount,
                    source.product_resolution,
                    source.source_kafka_timestamp,
                    current_timestamp(),
                    current_timestamp()
                )
            """
        )

        print(
            "[PASS] FACT_ORDER_ITEM MERGE completed."
        )

        # -----------------------------------------------------
        # Final Audit
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
            f"Null product_sk: "
            f"{null_products:,}"
        )

        print(
            f"Null order_sk: "
            f"{null_orders:,}"
        )

        print("\nPRODUCT RESOLUTION")

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

        source_df.unpersist()

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
