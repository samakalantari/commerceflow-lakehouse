from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    DIM_DATE,
    DIM_PRODUCT,
    DIM_USER,
    FACT_ORDER,
    FACT_ORDER_ITEM,
)


def count_duplicates(
    df: DataFrame,
    key: str,
) -> int:
    return (
        df
        .groupBy(key)
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )


def print_check(
    name: str,
    value: int,
    expected: int = 0,
) -> bool:
    passed = value == expected

    status = (
        "PASS"
        if passed
        else "FAIL"
    )

    print(
        f"[{status}] "
        f"{name}: "
        f"{value:,}"
    )

    return passed


def main() -> None:

    spark = build_iceberg_spark(
        "audit-silver-layer"
    )

    try:

        print("=" * 110)
        print("SILVER LAYER END-TO-END AUDIT")
        print("=" * 110)

        dim_date = spark.table(
            DIM_DATE
        )

        dim_user = spark.table(
            DIM_USER
        )

        dim_product = spark.table(
            DIM_PRODUCT
        )

        fact_order = spark.table(
            FACT_ORDER
        )

        fact_order_item = spark.table(
            FACT_ORDER_ITEM
        )

        all_passed = True

        # =====================================================
        # DIM_DATE
        # =====================================================

        print()
        print("DIM_DATE")
        print("-" * 110)

        dim_date_count = (
            dim_date.count()
        )

        print(
            f"Rows: "
            f"{dim_date_count:,}"
        )

        duplicate_date_sk = (
            count_duplicates(
                dim_date,
                "date_sk",
            )
        )

        all_passed &= print_check(
            "Duplicate date_sk",
            duplicate_date_sk,
        )

        null_date_sk = (
            dim_date
            .filter(
                F.col(
                    "date_sk"
                ).isNull()
            )
            .count()
        )

        all_passed &= print_check(
            "Null date_sk",
            null_date_sk,
        )

        # =====================================================
        # DIM_USER
        # =====================================================

        print()
        print("DIM_USER")
        print("-" * 110)

        dim_user_count = (
            dim_user.count()
        )

        print(
            f"Rows: "
            f"{dim_user_count:,}"
        )

        duplicate_user_id = (
            count_duplicates(
                dim_user,
                "user_id",
            )
        )

        all_passed &= print_check(
            "Duplicate user_id",
            duplicate_user_id,
        )

        duplicate_user_sk = (
            count_duplicates(
                dim_user,
                "user_sk",
            )
        )

        all_passed &= print_check(
            "Duplicate user_sk",
            duplicate_user_sk,
        )

        null_user_sk = (
            dim_user
            .filter(
                F.col(
                    "user_sk"
                ).isNull()
            )
            .count()
        )

        all_passed &= print_check(
            "Null user_sk",
            null_user_sk,
        )

        # =====================================================
        # DIM_PRODUCT SCD2
        # =====================================================

        print()
        print("DIM_PRODUCT SCD TYPE 2")
        print("-" * 110)

        dim_product_count = (
            dim_product.count()
        )

        distinct_products = (
            dim_product
            .select(
                "product_id"
            )
            .distinct()
            .count()
        )

        print(
            f"Rows: "
            f"{dim_product_count:,}"
        )

        print(
            f"Distinct products: "
            f"{distinct_products:,}"
        )

        duplicate_product_sk = (
            count_duplicates(
                dim_product,
                "product_sk",
            )
        )

        all_passed &= print_check(
            "Duplicate product_sk",
            duplicate_product_sk,
        )

        null_product_sk = (
            dim_product
            .filter(
                F.col(
                    "product_sk"
                ).isNull()
            )
            .count()
        )

        all_passed &= print_check(
            "Null product_sk",
            null_product_sk,
        )

        # Exactly one current version per product

        invalid_current_count = (
            dim_product
            .groupBy(
                "product_id"
            )
            .agg(
                F.sum(
                    F.when(
                        F.col(
                            "is_current"
                        ),
                        1,
                    ).otherwise(
                        0
                    )
                ).alias(
                    "current_count"
                )
            )
            .filter(
                F.col(
                    "current_count"
                ) != 1
            )
            .count()
        )

        all_passed &= print_check(
            "Products without exactly one current version",
            invalid_current_count,
        )

        # Current rows must have effective_to = NULL

        invalid_current_effective_to = (
            dim_product
            .filter(
                F.col(
                    "is_current"
                )
                &
                F.col(
                    "effective_to"
                ).isNotNull()
            )
            .count()
        )

        all_passed &= print_check(
            "Current rows with non-null effective_to",
            invalid_current_effective_to,
        )

        # Historical rows must have effective_to

        invalid_historical_effective_to = (
            dim_product
            .filter(
                ~F.col(
                    "is_current"
                )
                &
                F.col(
                    "effective_to"
                ).isNull()
            )
            .count()
        )

        all_passed &= print_check(
            "Historical rows with null effective_to",
            invalid_historical_effective_to,
        )

        # Check SCD2 interval overlap

        product_window = (
            Window
            .partitionBy(
                "product_id"
            )
            .orderBy(
                "effective_from"
            )
        )

        scd_intervals = (
            dim_product
            .withColumn(
                "next_effective_from",
                F.lead(
                    "effective_from"
                ).over(
                    product_window
                ),
            )
        )

        overlapping_intervals = (
            scd_intervals
            .filter(
                F.col(
                    "next_effective_from"
                ).isNotNull()
                &
                F.col(
                    "effective_to"
                ).isNotNull()
                &
                (
                    F.col(
                        "effective_to"
                    )
                    >
                    F.col(
                        "next_effective_from"
                    )
                )
            )
            .count()
        )

        all_passed &= print_check(
            "Overlapping SCD2 intervals",
            overlapping_intervals,
        )

        # =====================================================
        # FACT_ORDER
        # =====================================================

        print()
        print("FACT_ORDER")
        print("-" * 110)

        fact_order_count = (
            fact_order.count()
        )

        print(
            f"Rows: "
            f"{fact_order_count:,}"
        )

        duplicate_order_id = (
            count_duplicates(
                fact_order,
                "order_id",
            )
        )

        all_passed &= print_check(
            "Duplicate order_id",
            duplicate_order_id,
        )

        null_fact_user = (
            fact_order
            .filter(
                F.col(
                    "user_sk"
                ).isNull()
            )
            .count()
        )

        all_passed &= print_check(
            "Null user_sk in fact_order",
            null_fact_user,
        )

        null_order_date = (
            fact_order
            .filter(
                F.col(
                    "order_date_sk"
                ).isNull()
            )
            .count()
        )

        all_passed &= print_check(
            "Null order_date_sk in fact_order",
            null_order_date,
        )

        # FK fact_order -> dim_user

        orphan_order_users = (
            fact_order.alias("f")
            .join(
                dim_user.alias("d"),
                F.col(
                    "f.user_sk"
                )
                ==
                F.col(
                    "d.user_sk"
                ),
                how="left_anti",
            )
            .count()
        )

        all_passed &= print_check(
            "fact_order rows without dim_user",
            orphan_order_users,
        )

        # FK fact_order -> dim_date

        orphan_order_dates = (
            fact_order.alias("f")
            .join(
                dim_date.alias("d"),
                F.col(
                    "f.order_date_sk"
                )
                ==
                F.col(
                    "d.date_sk"
                ),
                how="left_anti",
            )
            .count()
        )

        all_passed &= print_check(
            "fact_order rows without dim_date",
            orphan_order_dates,
        )

        # =====================================================
        # FACT_ORDER_ITEM
        # =====================================================

        print()
        print("FACT_ORDER_ITEM")
        print("-" * 110)

        fact_item_count = (
            fact_order_item.count()
        )

        print(
            f"Rows: "
            f"{fact_item_count:,}"
        )

        duplicate_item_id = (
            count_duplicates(
                fact_order_item,
                "order_item_id",
            )
        )

        all_passed &= print_check(
            "Duplicate order_item_id",
            duplicate_item_id,
        )

        null_item_order = (
            fact_order_item
            .filter(
                F.col(
                    "order_sk"
                ).isNull()
            )
            .count()
        )

        all_passed &= print_check(
            "Null order_sk in fact_order_item",
            null_item_order,
        )

        null_item_product = (
            fact_order_item
            .filter(
                F.col(
                    "product_sk"
                ).isNull()
            )
            .count()
        )

        all_passed &= print_check(
            "Null product_sk in fact_order_item",
            null_item_product,
        )

        # FK fact_order_item -> fact_order

        orphan_item_orders = (
            fact_order_item.alias("i")
            .join(
                fact_order.alias("o"),
                F.col(
                    "i.order_sk"
                )
                ==
                F.col(
                    "o.order_sk"
                ),
                how="left_anti",
            )
            .count()
        )

        all_passed &= print_check(
            "fact_order_item rows without fact_order",
            orphan_item_orders,
        )

        # FK fact_order_item -> dim_product

        orphan_item_products = (
            fact_order_item.alias("i")
            .join(
                dim_product.alias("p"),
                F.col(
                    "i.product_sk"
                )
                ==
                F.col(
                    "p.product_sk"
                ),
                how="left_anti",
            )
            .count()
        )

        all_passed &= print_check(
            "fact_order_item rows without dim_product",
            orphan_item_products,
        )

        # FK fact_order_item -> dim_date

        orphan_item_dates = (
            fact_order_item.alias("i")
            .join(
                dim_date.alias("d"),
                F.col(
                    "i.order_date_sk"
                )
                ==
                F.col(
                    "d.date_sk"
                ),
                how="left_anti",
            )
            .count()
        )

        all_passed &= print_check(
            "fact_order_item rows without dim_date",
            orphan_item_dates,
        )

        # =====================================================
        # PRODUCT RESOLUTION
        # =====================================================

        print()
        print("PRODUCT RESOLUTION")
        print("-" * 110)

        (
            fact_order_item
            .groupBy(
                "product_resolution"
            )
            .count()
            .orderBy(
                F.col(
                    "count"
                ).desc()
            )
            .show(
                truncate=False
            )
        )

        # =====================================================
        # ICEBERG SNAPSHOTS
        # =====================================================

        print()
        print("ICEBERG SNAPSHOTS")
        print("-" * 110)

        tables = (
            DIM_DATE,
            DIM_USER,
            DIM_PRODUCT,
            FACT_ORDER,
            FACT_ORDER_ITEM,
        )

        for table in tables:

            snapshot_count = (
                spark.sql(
                    f"""
                    SELECT *
                    FROM {table}.snapshots
                    """
                )
                .count()
            )

            print(
                f"{table}: "
                f"{snapshot_count:,} snapshots"
            )

        # =====================================================
        # FINAL RESULT
        # =====================================================

        print()
        print("=" * 110)

        if all_passed:

            print(
                "ALL SILVER QUALITY CHECKS PASSED"
            )

            print(
                "Transactional Silver Layer "
                "is healthy."
            )

        else:

            print(
                "SILVER QUALITY CHECKS FAILED"
            )

            print(
                "Review the FAIL checks above."
            )

            raise RuntimeError(
                "Silver audit failed."
            )

        print("=" * 110)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
