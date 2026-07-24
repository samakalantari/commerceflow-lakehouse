from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_apps.silver.common.bronze_reader import (
    read_bronze_topic,
)
from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    TOPIC_PRODUCT_PRICE_HISTORY,
    TOPIC_PRODUCTS,
)


def main() -> None:
    spark = build_iceberg_spark("profile-product-history-consistency")

    try:
        products_df = read_bronze_topic(
            spark,
            TOPIC_PRODUCTS,
        )

        history_df = read_bronze_topic(
            spark,
            TOPIC_PRODUCT_PRICE_HISTORY,
        )

        print("=" * 100)
        print("PRODUCT PRICE HISTORY CONSISTENCY")
        print("=" * 100)

        # -------------------------------------------------
        # Latest version of each product
        # -------------------------------------------------

        product_window = Window.partitionBy("product_id").orderBy(
            F.col("kafka_timestamp").desc(),
            F.col("kafka_partition").desc(),
            F.col("kafka_offset").desc(),
        )

        latest_products = (
            products_df.withColumn(
                "_rn",
                F.row_number().over(product_window),
            )
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        # -------------------------------------------------
        # is_current distribution
        # -------------------------------------------------

        print("\nIS_CURRENT DISTRIBUTION")
        print("-" * 100)

        (history_df.groupBy("is_current").count().show(truncate=False))

        # -------------------------------------------------
        # Products missing price history
        # -------------------------------------------------

        products_without_history = latest_products.select("product_id").join(
            history_df.select("product_id").distinct(),
            on="product_id",
            how="left_anti",
        )

        missing_count = products_without_history.count()

        print(f"\nProducts without price history: {missing_count:,}")

        products_without_history.show(
            20,
            truncate=False,
        )

        # -------------------------------------------------
        # Duplicate price-history business events
        # -------------------------------------------------

        duplicate_history = (
            history_df.groupBy(
                "product_id",
                "valid_from",
                "price",
            )
            .count()
            .filter(F.col("count") > 1)
        )

        duplicate_count = duplicate_history.count()

        print(f"\nDuplicate history events: {duplicate_count:,}")

        duplicate_history.show(
            20,
            truncate=False,
        )

        # -------------------------------------------------
        # Latest history price versus product current price
        # -------------------------------------------------

        history_window = Window.partitionBy("product_id").orderBy(
            F.col("valid_from").desc(),
            F.col("kafka_timestamp").desc(),
            F.col("kafka_offset").desc(),
        )

        latest_history = (
            history_df.withColumn(
                "_rn",
                F.row_number().over(history_window),
            )
            .filter(F.col("_rn") == 1)
            .select(
                "product_id",
                F.col("price").alias("history_price"),
                "valid_from",
            )
        )

        price_mismatch = (
            latest_products.select(
                "product_id",
                F.col("price").alias("product_price"),
            )
            .join(
                latest_history,
                on="product_id",
                how="inner",
            )
            .filter(F.col("product_price") != F.col("history_price"))
        )

        mismatch_count = price_mismatch.count()

        print(f"\nCurrent price mismatches: {mismatch_count:,}")

        price_mismatch.show(
            20,
            truncate=False,
        )

        print()
        print("[PASS] PRODUCT HISTORY CONSISTENCY PROFILING COMPLETED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
