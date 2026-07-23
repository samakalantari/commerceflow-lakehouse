from pyspark.sql import functions as F

from spark_apps.gold.common.clickhouse import (
    read_clickhouse_table,
)
from spark_apps.gold.config.tables import (
    TRANSACTIONAL_OBT,
)
from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    FACT_ORDER,
    FACT_ORDER_ITEM,
)


def main() -> None:

    spark = build_iceberg_spark(
        "gold-transactional-audit"
    )

    try:

        print("=" * 100)
        print(
            "GOLD TRANSACTIONAL OBT AUDIT"
        )
        print("=" * 100)

        # ====================================================
        # Silver Sources
        # ====================================================

        fact_order = spark.table(
            FACT_ORDER
        )

        fact_order_item = spark.table(
            FACT_ORDER_ITEM
        )

        # ====================================================
        # Gold OBT grain is one row per order item.
        #
        # Therefore only orders that have at least one
        # fact_order_item are expected to exist in Gold.
        # Orders without items are intentionally not represented
        # in an item-grain OBT.
        # ====================================================

        source_item_count = (
            fact_order_item.count()
        )

        represented_order_keys = (
            fact_order_item
            .select(
                "order_sk"
            )
            .distinct()
        )

        represented_orders = (
            fact_order
            .join(
                represented_order_keys,
                on="order_sk",
                how="inner",
            )
            .cache()
        )

        source_order_count = (
            represented_orders.count()
        )

        source_order_total = (
            represented_orders
            .agg(
                F.sum(
                    "order_total"
                ).alias(
                    "value"
                )
            )
            .first()[
                "value"
            ]
        )

        orders_without_items = (
            fact_order
            .join(
                represented_order_keys,
                on="order_sk",
                how="left_anti",
            )
            .count()
        )

        # ====================================================
        # ClickHouse Gold
        # ====================================================

        gold_df = (
            read_clickhouse_table(
                spark,
                TRANSACTIONAL_OBT,
            )
            .cache()
        )

        gold_count = (
            gold_df.count()
        )

        gold_distinct_items = (
            gold_df
            .select(
                "order_item_sk"
            )
            .distinct()
            .count()
        )

        gold_distinct_orders = (
            gold_df
            .select(
                "order_id"
            )
            .distinct()
            .count()
        )

        duplicate_items = (
            gold_df
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

        order_flag_sum = (
            gold_df
            .agg(
                F.sum(
                    "order_count_flag"
                ).alias(
                    "value"
                )
            )
            .first()[
                "value"
            ]
        )

        gold_order_total_once = (
            gold_df
            .agg(
                F.sum(
                    "order_total_once"
                ).alias(
                    "value"
                )
            )
            .first()[
                "value"
            ]
        )

        # ====================================================
        # Report
        # ====================================================

        print()
        print(
            f"Silver order items: "
            f"{source_item_count:,}"
        )

        print(
            f"Gold rows: "
            f"{gold_count:,}"
        )

        print(
            f"Gold distinct items: "
            f"{gold_distinct_items:,}"
        )

        print(
            f"Duplicate Gold items: "
            f"{duplicate_items:,}"
        )

        print()
        print(
            f"Silver orders represented in item OBT: "
            f"{source_order_count:,}"
        )

        print(
            f"Silver orders without items "
            f"(not represented in OBT): "
            f"{orders_without_items:,}"
        )

        print(
            f"Gold distinct orders: "
            f"{gold_distinct_orders:,}"
        )

        print(
            f"SUM(order_count_flag): "
            f"{order_flag_sum:,}"
        )

        print()
        print(
            f"Silver order revenue: "
            f"{source_order_total}"
        )

        print(
            f"Gold order_total_once: "
            f"{gold_order_total_once}"
        )

        # ====================================================
        # Assertions
        # ====================================================

        all_passed = True

        all_passed &= (
            gold_count
            ==
            source_item_count
        )

        all_passed &= (
            gold_distinct_items
            ==
            source_item_count
        )

        all_passed &= (
            duplicate_items
            ==
            0
        )

        all_passed &= (
            gold_distinct_orders
            ==
            source_order_count
        )

        all_passed &= (
            order_flag_sum
            ==
            source_order_count
        )

        all_passed &= (
            gold_order_total_once
            ==
            source_order_total
        )

        print()
        print("GOLD SAMPLE")
        print("-" * 100)

        (
            gold_df
            .select(
                "order_id",
                "order_item_id",
                "full_date",
                "user_id",
                "loyalty_tier",
                "product_id",
                "product_name",
                "quantity",
                "unit_price",
                "item_total_amount",
                "order_count_flag",
                "order_total_once",
            )
            .limit(
                10
            )
            .show(
                truncate=False
            )
        )

        print()
        print("=" * 100)

        if all_passed:

            print(
                "ALL GOLD QUALITY CHECKS PASSED"
            )

            print(
                "Transactional Gold Layer "
                "is healthy."
            )

        else:

            print(
                "GOLD QUALITY CHECKS FAILED"
            )

            raise RuntimeError(
                "Gold audit failed."
            )

        print("=" * 100)

        gold_df.unpersist()
        represented_orders.unpersist()

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
