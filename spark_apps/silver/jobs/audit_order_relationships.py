from pyspark.sql import functions as F

from spark_apps.silver.common.bronze_reader import (
    read_bronze_topic,
)
from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    DIM_PRODUCT,
    DIM_USER,
    TOPIC_ORDERS,
    TOPIC_ORDER_ITEMS,
)


def main() -> None:

    spark = build_iceberg_spark(
        "audit-silver-order-relationships"
    )

    try:

        orders = read_bronze_topic(
            spark,
            TOPIC_ORDERS,
        )

        items = read_bronze_topic(
            spark,
            TOPIC_ORDER_ITEMS,
        )

        dim_user = spark.table(
            DIM_USER
        )

        dim_product = spark.table(
            DIM_PRODUCT
        )

        print("=" * 100)
        print("ORDER RELATIONSHIP AUDIT")
        print("=" * 100)

        # -----------------------------------------------------
        # 1. True duplicate orders
        # -----------------------------------------------------

        duplicate_orders = (
            orders
            .groupBy(
                "order_id"
            )
            .count()
            .filter(
                F.col("count") > 1
            )
            .count()
        )

        print(
            f"Duplicate order_id: "
            f"{duplicate_orders:,}"
        )

        # -----------------------------------------------------
        # 2. True duplicate order items
        # -----------------------------------------------------

        duplicate_items = (
            items
            .groupBy(
                "order_item_id"
            )
            .count()
            .filter(
                F.col("count") > 1
            )
            .count()
        )

        print(
            f"Duplicate order_item_id: "
            f"{duplicate_items:,}"
        )

        # -----------------------------------------------------
        # 3. Order items without parent order
        # -----------------------------------------------------

        orphan_items = (
            items
            .select(
                "order_item_id",
                "order_id",
            )
            .join(
                orders
                .select(
                    "order_id"
                )
                .distinct(),
                on="order_id",
                how="left_anti",
            )
        )

        orphan_items_count = (
            orphan_items.count()
        )

        print(
            f"Order items without order: "
            f"{orphan_items_count:,}"
        )

        # -----------------------------------------------------
        # 4. Orders without items
        # -----------------------------------------------------

        orders_without_items = (
            orders
            .select(
                "order_id"
            )
            .join(
                items
                .select(
                    "order_id"
                )
                .distinct(),
                on="order_id",
                how="left_anti",
            )
        )

        orders_without_items_count = (
            orders_without_items.count()
        )

        print(
            f"Orders without items: "
            f"{orders_without_items_count:,}"
        )

        if orders_without_items_count > 0:

            orders_without_items.show(
                20,
                truncate=False,
            )

        # -----------------------------------------------------
        # 5. Users missing from DIM_USER
        # -----------------------------------------------------

        missing_users = (
            orders
            .select(
                "user_id"
            )
            .distinct()
            .join(
                dim_user
                .select(
                    "user_id"
                )
                .distinct(),
                on="user_id",
                how="left_anti",
            )
        )

        missing_users_count = (
            missing_users.count()
        )

        print(
            f"Order users missing from DIM_USER: "
            f"{missing_users_count:,}"
        )

        # -----------------------------------------------------
        # 6. Products missing completely from DIM_PRODUCT
        # -----------------------------------------------------

        missing_products = (
            items
            .select(
                "product_id"
            )
            .distinct()
            .join(
                dim_product
                .select(
                    "product_id"
                )
                .distinct(),
                on="product_id",
                how="left_anti",
            )
        )

        missing_products_count = (
            missing_products.count()
        )

        print(
            f"Products missing from DIM_PRODUCT: "
            f"{missing_products_count:,}"
        )

        # -----------------------------------------------------
        # 7. Attach business order timestamp to order items
        # -----------------------------------------------------

        items_with_time = (
            items
            .join(
                orders
                .select(
                    "order_id",
                    F.col(
                        "timestamp"
                    ).alias(
                        "order_timestamp"
                    ),
                ),
                on="order_id",
                how="left",
            )
        )

        missing_order_time = (
            items_with_time
            .filter(
                F.col(
                    "order_timestamp"
                ).isNull()
            )
            .count()
        )

        print(
            f"Items without order timestamp: "
            f"{missing_order_time:,}"
        )

        # -----------------------------------------------------
        # 8. Temporal lookup against DIM_PRODUCT SCD2
        # -----------------------------------------------------

        temporal_matches = (
            items_with_time.alias("i")
            .join(
                dim_product.alias("p"),
                (
                    F.col(
                        "i.product_id"
                    )
                    ==
                    F.col(
                        "p.product_id"
                    )
                )
                &
                (
                    F.col(
                        "i.order_timestamp"
                    )
                    >=
                    F.col(
                        "p.effective_from"
                    )
                )
                &
                (
                    F.col(
                        "p.effective_to"
                    ).isNull()
                    |
                    (
                        F.col(
                            "i.order_timestamp"
                        )
                        <
                        F.col(
                            "p.effective_to"
                        )
                    )
                ),
                how="left",
            )
            .select(
                F.col(
                    "i.order_item_id"
                ).alias(
                    "order_item_id"
                ),
                F.col(
                    "p.product_sk"
                ).alias(
                    "product_sk"
                ),
            )
        )

        temporal_match_counts = (
            temporal_matches
            .groupBy(
                "order_item_id"
            )
            .agg(
                F.count(
                    "product_sk"
                ).alias(
                    "match_count"
                )
            )
        )

        missing_temporal_product = (
            temporal_match_counts
            .filter(
                F.col(
                    "match_count"
                ) == 0
            )
            .count()
        )

        multiple_temporal_product = (
            temporal_match_counts
            .filter(
                F.col(
                    "match_count"
                ) > 1
            )
            .count()
        )

        print(
            f"Items without temporal product match: "
            f"{missing_temporal_product:,}"
        )

        print(
            f"Items with multiple temporal matches: "
            f"{multiple_temporal_product:,}"
        )

        # -----------------------------------------------------
        # 9. Item amount consistency
        #
        # Only profiling.
        # A difference may represent discounts.
        # -----------------------------------------------------

        item_amount_mismatch = (
            items
            .filter(
                F.abs(
                    F.col(
                        "item_total_amount"
                    )
                    -
                    (
                        F.col(
                            "quantity"
                        )
                        *
                        F.col(
                            "unit_price"
                        )
                    )
                )
                > F.lit(0.01)
            )
            .count()
        )

        print(
            f"Items where total != quantity * unit_price: "
            f"{item_amount_mismatch:,}"
        )

        # -----------------------------------------------------
        # 10. Order total vs sum item totals
        #
        # Again informational only.
        # Difference may include delivery, tax, discounts, etc.
        # -----------------------------------------------------

        item_totals = (
            items
            .groupBy(
                "order_id"
            )
            .agg(
                F.sum(
                    "item_total_amount"
                ).alias(
                    "calculated_items_total"
                )
            )
        )

        order_total_mismatch = (
            orders
            .select(
                "order_id",
                "total",
            )
            .join(
                item_totals,
                on="order_id",
                how="inner",
            )
            .filter(
                F.abs(
                    F.col("total")
                    -
                    F.col(
                        "calculated_items_total"
                    )
                )
                > F.lit(0.01)
            )
            .count()
        )

        print(
            f"Orders where total != sum(item_total): "
            f"{order_total_mismatch:,}"
        )

        print()
        print("=" * 100)
        print(
            "[PASS] ORDER RELATIONSHIP AUDIT COMPLETED"
        )
        print("=" * 100)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
