from pyspark.sql import functions as F

from spark_apps.silver.common.bronze_reader import (
    read_bronze_topic,
)
from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    TOPIC_CATEGORIES,
    TOPIC_PRODUCTS,
    TOPIC_PRODUCT_PRICE_HISTORY,
)


def profile_topic(
    spark,
    topic: str,
) -> None:
    print("\n")
    print("=" * 110)
    print(f"PROFILE: {topic}")
    print("=" * 110)

    df = read_bronze_topic(
        spark,
        topic,
    )

    count = df.count()

    print(
        f"Total records: {count:,}"
    )

    print("\nSCHEMA")
    print("-" * 110)

    df.printSchema()

    print("\nNULL COUNTS")
    print("-" * 110)

    business_columns = [
        column
        for column in df.columns
        if column not in {
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingested_at",
            "year",
            "month",
            "day",
        }
    ]

    if business_columns:
        (
            df.select(
                *[
                    F.sum(
                        F.when(
                            F.col(column).isNull(),
                            1,
                        ).otherwise(0)
                    ).alias(
                        column
                    )
                    for column
                    in business_columns
                ]
            )
            .show(
                truncate=False
            )
        )

    print("\nSAMPLE")
    print("-" * 110)

    (
        df
        .orderBy(
            F.col(
                "kafka_timestamp"
            ).desc()
        )
        .limit(5)
        .show(
            truncate=False,
            vertical=True,
        )
    )

    # ---------------------------------------------------------
    # Product-specific profiling
    # ---------------------------------------------------------

    if "product_id" in df.columns:

        distinct_products = (
            df
            .select(
                "product_id"
            )
            .distinct()
            .count()
        )

        print(
            f"\nDistinct product_id: "
            f"{distinct_products:,}"
        )

        print(
            f"Repeated product records: "
            f"{count - distinct_products:,}"
        )

        print(
            "\nPRODUCTS WITH MULTIPLE RECORDS"
        )

        (
            df
            .groupBy(
                "product_id"
            )
            .count()
            .filter(
                F.col("count") > 1
            )
            .orderBy(
                F.col("count").desc()
            )
            .show(
                20,
                truncate=False,
            )
        )

    # ---------------------------------------------------------
    # Category-specific profiling
    # ---------------------------------------------------------

    if "category_id" in df.columns:

        distinct_categories = (
            df
            .select(
                "category_id"
            )
            .distinct()
            .count()
        )

        print(
            f"\nDistinct category_id: "
            f"{distinct_categories:,}"
        )

    # ---------------------------------------------------------
    # Price profiling
    # ---------------------------------------------------------

    price_columns = [
        column
        for column in (
            "price",
            "unit_price",
            "old_price",
            "new_price",
        )
        if column in df.columns
    ]

    for price_column in price_columns:

        print(
            f"\nPRICE PROFILE: "
            f"{price_column}"
        )

        (
            df
            .select(
                F.min(
                    price_column
                ).alias(
                    "min"
                ),
                F.max(
                    price_column
                ).alias(
                    "max"
                ),
                F.avg(
                    price_column
                ).alias(
                    "avg"
                ),
            )
            .show(
                truncate=False
            )
        )

        invalid_price_count = (
            df
            .filter(
                F.col(
                    price_column
                ) < 0
            )
            .count()
        )

        print(
            f"Negative {price_column}: "
            f"{invalid_price_count:,}"
        )

    # ---------------------------------------------------------
    # Timestamp ranges
    # ---------------------------------------------------------

    timestamp_columns = [
        column
        for column in df.columns
        if (
            "timestamp" in column.lower()
            or "date" in column.lower()
            or "effective" in column.lower()
        )
    ]

    print(
        "\nDATE / TIMESTAMP COLUMNS"
    )
    print(
        timestamp_columns
    )


def main() -> None:
    spark = build_iceberg_spark(
        "profile-silver-products"
    )

    try:
        topics = (
            TOPIC_PRODUCTS,
            TOPIC_CATEGORIES,
            TOPIC_PRODUCT_PRICE_HISTORY,
        )

        for topic in topics:
            profile_topic(
                spark,
                topic,
            )

        print("\n")
        print("=" * 110)
        print(
            "[PASS] PRODUCT SOURCES PROFILING COMPLETED"
        )
        print("=" * 110)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
