from pyspark.sql import functions as F

from spark_apps.silver.common.bronze_reader import (
    read_bronze_topic,
)
from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    DIM_PRODUCT,
    TOPIC_PRODUCTS,
    TOPIC_PRODUCT_PRICE_HISTORY,
)
from spark_apps.silver.dimensions.dim_product import (
    build_dim_product_source,
)


def main() -> None:
    spark = build_iceberg_spark(
        "silver-load-dim-product"
    )

    try:
        print("=" * 100)
        print("BUILDING DIM_PRODUCT SCD TYPE 2")
        print("=" * 100)

        # -----------------------------------------------------
        # 1. Read Bronze sources
        # -----------------------------------------------------

        products_df = read_bronze_topic(
            spark,
            TOPIC_PRODUCTS,
        )

        history_df = read_bronze_topic(
            spark,
            TOPIC_PRODUCT_PRICE_HISTORY,
        )

        # -----------------------------------------------------
        # 2. Build canonical SCD Type 2 source
        # -----------------------------------------------------

        source_df = (
            build_dim_product_source(
                products_df,
                history_df,
            )
            .cache()
        )

        source_count = (
            source_df.count()
        )

        distinct_products = (
            source_df
            .select(
                "product_id"
            )
            .distinct()
            .count()
        )

        current_count = (
            source_df
            .filter(
                F.col(
                    "is_current"
                )
            )
            .count()
        )

        print(
            f"SCD2 versions: "
            f"{source_count:,}"
        )

        print(
            f"Distinct products: "
            f"{distinct_products:,}"
        )

        print(
            f"Current versions: "
            f"{current_count:,}"
        )

        # -----------------------------------------------------
        # 3. Create DIM_PRODUCT Iceberg table
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
        # 4. Create staging view
        # -----------------------------------------------------

        source_df.createOrReplaceTempView(
            "staged_dim_product"
        )

        # -----------------------------------------------------
        # 5. Idempotent Iceberg MERGE
        #
        # Same product_sk + no changes:
        #     no update
        #
        # Same product_sk + changed attributes:
        #     update
        #
        # New product_sk:
        #     insert
        #
        # <=> is Spark SQL null-safe equality.
        # This is important for effective_to.
        # -----------------------------------------------------

        spark.sql(
            f"""
            MERGE INTO
                {DIM_PRODUCT} AS target

            USING
                staged_dim_product AS source

            ON
                target.product_sk =
                source.product_sk

            WHEN MATCHED
            AND (
                NOT (
                    target.product_name
                    <=>
                    source.product_name
                )

                OR NOT (
                    target.price
                    <=>
                    source.price
                )

                OR NOT (
                    target.effective_from
                    <=>
                    source.effective_from
                )

                OR NOT (
                    target.effective_to
                    <=>
                    source.effective_to
                )

                OR NOT (
                    target.is_current
                    <=>
                    source.is_current
                )

                OR NOT (
                    target.record_hash
                    <=>
                    source.record_hash
                )

                OR NOT (
                    target.source_kind
                    <=>
                    source.source_kind
                )

                OR NOT (
                    target.source_kafka_timestamp
                    <=>
                    source.source_kafka_timestamp
                )
            )

            THEN UPDATE SET

                target.product_name =
                    source.product_name,

                target.price =
                    source.price,

                target.effective_from =
                    source.effective_from,

                target.effective_to =
                    source.effective_to,

                target.is_current =
                    source.is_current,

                target.record_hash =
                    source.record_hash,

                target.source_kind =
                    source.source_kind,

                target.source_kafka_timestamp =
                    source.source_kafka_timestamp,

                target.silver_updated_at =
                    current_timestamp()

            WHEN NOT MATCHED THEN

                INSERT (
                    product_sk,
                    product_id,
                    product_name,
                    price,
                    effective_from,
                    effective_to,
                    is_current,
                    record_hash,
                    source_kind,
                    source_kafka_timestamp,
                    silver_created_at,
                    silver_updated_at
                )

                VALUES (
                    source.product_sk,
                    source.product_id,
                    source.product_name,
                    source.price,
                    source.effective_from,
                    source.effective_to,
                    source.is_current,
                    source.record_hash,
                    source.source_kind,
                    source.source_kafka_timestamp,
                    current_timestamp(),
                    current_timestamp()
                )
            """
        )

        print(
            "[PASS] DIM_PRODUCT MERGE completed."
        )

        # -----------------------------------------------------
        # 6. Final Audit
        # -----------------------------------------------------

        dim_df = spark.table(
            DIM_PRODUCT
        )

        silver_count = (
            dim_df.count()
        )

        silver_products = (
            dim_df
            .select(
                "product_id"
            )
            .distinct()
            .count()
        )

        silver_current = (
            dim_df
            .filter(
                F.col(
                    "is_current"
                )
            )
            .count()
        )

        invalid_current = (
            dim_df
            .filter(
                F.col(
                    "is_current"
                )
            )
            .groupBy(
                "product_id"
            )
            .count()
            .filter(
                F.col(
                    "count"
                ) != 1
            )
            .count()
        )

        duplicate_product_sk = (
            dim_df
            .groupBy(
                "product_sk"
            )
            .count()
            .filter(
                F.col(
                    "count"
                ) > 1
            )
            .count()
        )

        print()
        print("DIM_PRODUCT AUDIT")
        print("-" * 100)

        print(
            f"Total SCD2 rows: "
            f"{silver_count:,}"
        )

        print(
            f"Distinct products: "
            f"{silver_products:,}"
        )

        print(
            f"Current rows: "
            f"{silver_current:,}"
        )

        print(
            f"Products with invalid "
            f"current-version count: "
            f"{invalid_current:,}"
        )

        print(
            f"Duplicate product_sk: "
            f"{duplicate_product_sk:,}"
        )

        # -----------------------------------------------------
        # 7. Current product sample
        # -----------------------------------------------------

        print()
        print("CURRENT PRODUCT SAMPLE")

        (
            dim_df
            .filter(
                F.col(
                    "is_current"
                )
            )
            .select(
                "product_sk",
                "product_id",
                "product_name",
                "price",
                "effective_from",
                "effective_to",
                "source_kind",
            )
            .limit(
                10
            )
            .show(
                truncate=False
            )
        )

        # -----------------------------------------------------
        # 8. SCD2 history sample
        # -----------------------------------------------------

        print()
        print("SCD2 HISTORY SAMPLE")

        product_with_history = (
            dim_df
            .groupBy(
                "product_id"
            )
            .count()
            .filter(
                F.col(
                    "count"
                ) > 1
            )
            .orderBy(
                F.col(
                    "count"
                ).desc()
            )
            .select(
                "product_id"
            )
            .limit(
                1
            )
            .collect()
        )

        if product_with_history:

            sample_product = (
                product_with_history[0][
                    "product_id"
                ]
            )

            print(
                f"Product: "
                f"{sample_product}"
            )

            (
                dim_df
                .filter(
                    F.col(
                        "product_id"
                    )
                    ==
                    sample_product
                )
                .orderBy(
                    "effective_from"
                )
                .show(
                    truncate=False
                )
            )

        # -----------------------------------------------------
        # 9. Final result
        # -----------------------------------------------------

        print()
        print("=" * 100)

        if (
            silver_products
            ==
            silver_current
            and
            invalid_current
            ==
            0
            and
            duplicate_product_sk
            ==
            0
        ):

            print(
                "[PASS] DIM_PRODUCT SCD2 "
                "LOAD COMPLETED"
            )

        else:

            print(
                "[FAIL] DIM_PRODUCT SCD2 "
                "AUDIT FAILED"
            )

            raise RuntimeError(
                "DIM_PRODUCT audit failed."
            )

        print("=" * 100)

        source_df.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
