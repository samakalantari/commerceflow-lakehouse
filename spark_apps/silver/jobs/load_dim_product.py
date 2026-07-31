from pyspark.sql import functions as F

from spark_apps.silver.common.bronze_reader import (
    bronze_topic_day_path,
    read_bronze_topic,
    split_tombstones,
    apply_tombstone_deletes,
)
from spark_apps.silver.common.job_arguments import get_source_selection
from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    DIM_PRODUCT,
    FACT_ORDER_ITEM,
    INVALID_PRODUCTS,
    QUARANTINE_DATABASE,
    TOPIC_PRODUCT_PRICE_HISTORY,
    TOPIC_PRODUCTS,
)
from spark_apps.silver.dimensions.dim_product import (
    add_unknown_product_member,
    build_dim_product_source,
)
from spark_apps.silver.quality.quarantine import (
    prepare_quarantine_records,
    write_quarantine,
)


def main() -> None:

    source = get_source_selection()
    spark = build_iceberg_spark("silver-load-dim-product")

    try:
        print("=" * 100)
        print("BUILDING DIM_PRODUCT SCD TYPE 2")
        print("=" * 100)

        # -----------------------------------------------------
        # 1. Read Bronze sources
        # -----------------------------------------------------

        for topic in (TOPIC_PRODUCTS, TOPIC_PRODUCT_PRICE_HISTORY):
            print(f"Bronze source mode for {topic}: {source.mode}")

        products_df = read_bronze_topic(
            spark,
            TOPIC_PRODUCTS,
            ingested_date=source.ingested_date,
            source_mode=source.mode,
        ).select(
            "product_id", "name", "price",
            "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset",
            "kafka_timestamp", "ingested_at",
        )

        history_df = read_bronze_topic(
            spark,
            TOPIC_PRODUCT_PRICE_HISTORY,
            ingested_date=source.ingested_date,
            source_mode=source.mode,
        ).select(
            "product_id", "price", "valid_from",
            "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset",
            "kafka_timestamp", "ingested_at",
        )
        products_df, product_tombstones = split_tombstones(
            products_df,
            business_key="product_id",
            payload_columns=("product_id", "name", "price"),
        )
        history_df, history_tombstones = split_tombstones(
            history_df,
            business_key="product_id",
            payload_columns=("product_id", "price", "valid_from"),
        )
        product_tombstones = product_tombstones.unionByName(history_tombstones).distinct()

        # -----------------------------------------------------
        # 2. Build canonical SCD Type 2 source
        # -----------------------------------------------------

        source_df, invalid_df = build_dim_product_source(
            products_df,
            history_df,
            persist_classified=True,
        )

        source_df = add_unknown_product_member(source_df)

        source_df = source_df.cache()

        source_count = source_df.count()

        distinct_products = (
            source_df
            .select("product_id")
            .distinct()
            .count()
        )

        current_count = (
            source_df
            .filter(F.col("is_current"))
            .count()
        )

        invalid_count = invalid_df.count()

        print(f"SCD2 versions: {source_count:,}")
        print(f"Distinct products: {distinct_products:,}")
        print(f"Current versions: {current_count:,}")

        # -----------------------------------------------------
        # 3. Write invalid records to quarantine
        # -----------------------------------------------------

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {QUARANTINE_DATABASE}")

        quarantine_df = prepare_quarantine_records(
            invalid_df,
            entity_name="product",
        )

        write_quarantine(
            quarantine_df,
            INVALID_PRODUCTS,
        )

        print(f"[INFO] Current invalid products in quarantine: {invalid_count:,}")

        # -----------------------------------------------------
        # 4. Create DIM_PRODUCT Iceberg table
        # -----------------------------------------------------

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS
            {DIM_PRODUCT}
            (
                product_sk BIGINT,
                product_id STRING,
                product_name STRING,
                price DECIMAL(10,2),
                effective_from TIMESTAMP,
                effective_to TIMESTAMP,
                is_current BOOLEAN,
                record_hash STRING,
                source_kind STRING,
                source_kafka_timestamp TIMESTAMP,
                silver_created_at TIMESTAMP,
                silver_updated_at TIMESTAMP
            )
            USING iceberg
            TBLPROPERTIES (
                'format-version' = '2'
            )
            """
        )

        # -----------------------------------------------------
        # 5. Source audit
        # -----------------------------------------------------

        source_invalid_current = (
            source_df.groupBy("product_id")
            .agg(
                F.sum(
                    F.when(
                        F.col("is_current"),
                        1,
                    ).otherwise(0)
                ).alias("current_count")
            )
            .filter(F.col("current_count") != 1)
            .count()
        )

        source_duplicate_product_sk = (
            source_df.groupBy("product_sk").count().filter(F.col("count") > 1).count()
        )

        print()
        print("DIM_PRODUCT SOURCE AUDIT")
        print("-" * 100)

        print(f"Source rows: {source_count:,}")

        print(f"Distinct products: {distinct_products:,}")

        print(f"Current versions: {current_count:,}")

        print(f"Products with invalid current-version count: {source_invalid_current:,}")

        print(f"Duplicate product_sk: {source_duplicate_product_sk:,}")

        if (
            current_count != distinct_products
            or source_invalid_current != 0
            or source_duplicate_product_sk != 0
        ):
            raise RuntimeError(
                "DIM_PRODUCT canonical source audit failed."
            )

        print("[PASS] DIM_PRODUCT canonical source audit completed.")

        # -----------------------------------------------------
        # 6. Prepare final Silver rows
        # -----------------------------------------------------

        write_df = (
            source_df.withColumn(
                "silver_created_at",
                F.current_timestamp(),
            )
            .withColumn(
                "silver_updated_at",
                F.current_timestamp(),
            )
            .select(
                "product_sk",
                "product_id",
                "product_name",
                "price",
                "effective_from",
                "effective_to",
                "is_current",
                "record_hash",
                "source_kind",
                "source_kafka_timestamp",
                "silver_created_at",
                "silver_updated_at",
            )
        )

        if source.mode == "daily":
            product_tombstones.createOrReplaceTempView("deleted_products")
            spark.sql(
                f"""
                MERGE INTO {FACT_ORDER_ITEM} AS target
                USING deleted_products AS source
                ON target.product_id = source.product_id
                WHEN MATCHED THEN UPDATE SET
                    target.product_sk = CAST(0 AS BIGINT),
                    target.product_resolution = 'unknown_product',
                    target.silver_updated_at = current_timestamp()
                """
            )
            apply_tombstone_deletes(
                spark, table_name=DIM_PRODUCT, business_key="product_id",
                tombstones=product_tombstones, view_name="deleted_products"
            )

        write_df.createOrReplaceTempView("staged_dim_product")

        spark.sql(
            f"""
            MERGE INTO {DIM_PRODUCT} AS target
            USING (
                SELECT product_id, min(effective_from) AS effective_from
                FROM staged_dim_product
                WHERE product_id <> '__UNKNOWN__'
                GROUP BY product_id
            ) AS source
            ON target.product_id = source.product_id
               AND target.is_current
               AND target.effective_from < source.effective_from
            WHEN MATCHED THEN UPDATE SET
                target.effective_to = source.effective_from,
                target.is_current = false,
                target.silver_updated_at = current_timestamp()
            """
        )

        # -----------------------------------------------------
        # 7. Full overwrite
        # -----------------------------------------------------

        spark.sql(
            f"""
            MERGE INTO {DIM_PRODUCT} AS target
            USING staged_dim_product AS source
            ON target.product_sk = source.product_sk
            WHEN MATCHED AND source.source_kafka_timestamp > target.source_kafka_timestamp
                THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )

        print("[PASS] DIM_PRODUCT incremental MERGE completed.")

        # -----------------------------------------------------
        # 8. Final audit
        # -----------------------------------------------------

        dim_df = spark.table(DIM_PRODUCT)

        silver_count = dim_df.count()

        silver_products = dim_df.select("product_id").distinct().count()

        silver_current = dim_df.filter(F.col("is_current")).count()

        invalid_current = (
            dim_df.groupBy("product_id")
            .agg(
                F.sum(
                    F.when(
                        F.col("is_current"),
                        1,
                    ).otherwise(0)
                ).alias("current_count")
            )
            .filter(F.col("current_count") != 1)
            .count()
        )

        duplicate_product_sk = (
            dim_df.groupBy("product_sk").count().filter(F.col("count") > 1).count()
        )

        print()
        print("DIM_PRODUCT AUDIT")
        print("-" * 100)

        print(f"Total SCD2 rows: {silver_count:,}")

        print(f"Distinct products: {silver_products:,}")

        print(f"Current rows: {silver_current:,}")

        print(f"Products with invalid current-version count: {invalid_current:,}")

        print(f"Duplicate product_sk: {duplicate_product_sk:,}")

        print("=" * 100)

        if silver_products == silver_current and invalid_current == 0 and duplicate_product_sk == 0:
            print("[PASS] DIM_PRODUCT SCD2 LOAD COMPLETED")

        else:
            raise RuntimeError(
                "DIM_PRODUCT audit failed."
            )

        print("=" * 100)

        source_df.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
