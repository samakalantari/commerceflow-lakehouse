from pyspark.sql import functions as F

from spark_apps.silver.common.bronze_reader import (
    read_bronze_topic,
)
from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    TOPIC_ORDER_ITEMS,
    TOPIC_ORDERS,
)

METADATA_COLUMNS = {
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


def profile_topic(
    spark,
    topic: str,
) -> None:

    print()
    print("=" * 110)
    print(f"PROFILE: {topic}")
    print("=" * 110)

    df = read_bronze_topic(
        spark,
        topic,
    )

    total_count = df.count()

    print(f"Total records: {total_count:,}")

    print("\nSCHEMA")
    print("-" * 110)

    df.printSchema()

    business_columns = [column for column in df.columns if column not in METADATA_COLUMNS]

    print("\nNULL COUNTS")
    print("-" * 110)

    (
        df.select(
            *[
                F.sum(
                    F.when(
                        F.col(column).isNull(),
                        1,
                    ).otherwise(0)
                ).alias(column)
                for column in business_columns
            ]
        ).show(truncate=False)
    )

    print("\nSAMPLE")
    print("-" * 110)

    (
        df.orderBy(F.col("kafka_timestamp").desc())
        .limit(5)
        .show(
            truncate=False,
            vertical=True,
        )
    )

    # ---------------------------------------------------------
    # Order ID profiling
    # ---------------------------------------------------------

    if "order_id" in df.columns:
        distinct_orders = df.select("order_id").distinct().count()

        print(f"\nDistinct order_id: {distinct_orders:,}")

        print(f"Repeated order records: {total_count - distinct_orders:,}")

        print("\nTOP REPEATED ORDERS")

        (
            df.groupBy("order_id")
            .count()
            .filter(F.col("count") > 1)
            .orderBy(F.col("count").desc())
            .show(
                20,
                truncate=False,
            )
        )

    # ---------------------------------------------------------
    # Numeric columns
    # ---------------------------------------------------------

    numeric_candidates = (
        "quantity",
        "price",
        "unit_price",
        "total_amount",
        "amount",
        "discount",
    )

    for column in numeric_candidates:
        if column not in df.columns:
            continue

        print(f"\nNUMERIC PROFILE: {column}")

        (
            df.select(
                F.min(column).alias("min"),
                F.max(column).alias("max"),
                F.avg(column).alias("avg"),
            ).show(truncate=False)
        )

        invalid_count = df.filter(F.col(column) < 0).count()

        print(f"Negative {column}: {invalid_count:,}")

    # ---------------------------------------------------------
    # Distinct values for likely status columns
    # ---------------------------------------------------------

    for column in (
        "status",
        "order_status",
        "payment_method",
    ):
        if column not in df.columns:
            continue

        print(f"\nVALUES: {column}")

        (
            df.groupBy(column)
            .count()
            .orderBy(F.col("count").desc())
            .show(
                50,
                truncate=False,
            )
        )


def main() -> None:

    spark = build_iceberg_spark("profile-silver-orders")

    try:
        profile_topic(
            spark,
            TOPIC_ORDERS,
        )

        profile_topic(
            spark,
            TOPIC_ORDER_ITEMS,
        )

        print()
        print("=" * 110)
        print("[PASS] ORDER SOURCES PROFILING COMPLETED")
        print("=" * 110)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
