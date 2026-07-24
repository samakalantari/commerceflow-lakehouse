from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_dim_product_source(
    products_df: DataFrame,
    price_history_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Build the canonical Product SCD Type 2 source.

    Product snapshots and price-history records are normalized,
    deduplicated and validated before building the SCD2 timeline.

    Returns:
        valid_df:
            Canonical Product SCD2 records.

        invalid_df:
            Invalid product snapshots and price-history records
            with data-quality error reasons.
    """

    # =========================================================
    # 1. Normalize product snapshots
    # =========================================================

    normalized_products = (
        products_df.withColumn(
            "product_id",
            F.trim(F.col("product_id")),
        )
        .withColumn(
            "product_name",
            F.trim(F.col("name")),
        )
        .withColumn(
            "snapshot_price",
            F.col("price"),
        )
        .withColumn(
            "snapshot_timestamp",
            F.col("kafka_timestamp"),
        )
    )

    # =========================================================
    # 2. Separate snapshots without a usable business key
    #
    # Records without product_id must not be deduplicated
    # together because all NULL IDs would fall into one window.
    # =========================================================

    identified_products = normalized_products.filter(
        F.col("product_id").isNotNull()
        & (F.length(F.col("product_id")) > 0)
    )

    unidentified_products = normalized_products.filter(
        F.col("product_id").isNull()
        | (F.length(F.col("product_id")) == 0)
    )

    # =========================================================
    # 3. Keep latest snapshot for each valid product_id
    # =========================================================

    product_window = Window.partitionBy(
        "product_id"
    ).orderBy(
        F.col("kafka_timestamp").desc_nulls_last(),
        F.col("kafka_partition").desc_nulls_last(),
        F.col("kafka_offset").desc_nulls_last(),
    )

    latest_identified_products = (
        identified_products.withColumn(
            "_row_number",
            F.row_number().over(product_window),
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop(
            "_row_number"
        )
    )

    # Preserve every record with a missing product_id so each
    # invalid source message remains visible for later quarantine.
    latest_products = latest_identified_products.unionByName(
        unidentified_products,
        allowMissingColumns=True,
    )

    # =========================================================
    # 4. Validate latest product snapshots
    # =========================================================

    validated_products = latest_products.withColumn(
        "_dq_error_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("product_id").isNull()
                | (F.length(F.col("product_id")) == 0),
                F.lit("missing_product_id"),
            ),
            F.when(
                F.col("product_name").isNull()
                | (F.length(F.col("product_name")) == 0),
                F.lit("missing_product_name"),
            ),
            F.when(
                F.col("snapshot_price").isNull(),
                F.lit("missing_product_price"),
            ),
            F.when(
                F.col("snapshot_price") < 0,
                F.lit("negative_product_price"),
            ),
            F.when(
                F.col("snapshot_timestamp").isNull(),
                F.lit("missing_product_timestamp"),
            ),
        ),
    )

    valid_products = validated_products.filter(
        F.col("_dq_error_reason") == ""
    )

    invalid_products = (
        validated_products.filter(
            F.col("_dq_error_reason") != ""
        )
        .withColumn(
            "_dq_source_entity",
            F.lit("product_snapshot"),
        )
    )

    # =========================================================
    # 5. Normalize price-history records
    # =========================================================

    normalized_history = (
        price_history_df.withColumn(
            "product_id",
            F.trim(F.col("product_id")),
        )
        .withColumn(
            "effective_from",
            F.col("valid_from"),
        )
        .withColumn(
            "source_kafka_timestamp",
            F.col("kafka_timestamp"),
        )
    )

    # =========================================================
    # 6. Validate price-history records
    # =========================================================

    validated_history = normalized_history.withColumn(
        "_dq_error_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("product_id").isNull()
                | (F.length(F.col("product_id")) == 0),
                F.lit("missing_product_id"),
            ),
            F.when(
                F.col("price").isNull(),
                F.lit("missing_price"),
            ),
            F.when(
                F.col("price") < 0,
                F.lit("negative_price"),
            ),
            F.when(
                F.col("effective_from").isNull(),
                F.lit("missing_valid_from"),
            ),
            F.when(
                F.col("source_kafka_timestamp").isNull(),
                F.lit("missing_kafka_timestamp"),
            ),
        ),
    )

    valid_history = validated_history.filter(
        F.col("_dq_error_reason") == ""
    )

    invalid_history = (
        validated_history.filter(
            F.col("_dq_error_reason") != ""
        )
        .withColumn(
            "_dq_source_entity",
            F.lit("product_price_history"),
        )
    )

    # =========================================================
    # 7. Prepare valid price-history events
    # =========================================================

    history_events = valid_history.select(
        "product_id",
        "price",
        "effective_from",
        "source_kafka_timestamp",
        F.lit("price_history").alias("source_kind"),
    )

    # =========================================================
    # 8. Find latest historical price per product
    # =========================================================

    latest_history_window = Window.partitionBy(
        "product_id"
    ).orderBy(
        F.col("effective_from").desc_nulls_last(),
        F.col("source_kafka_timestamp").desc_nulls_last(),
    )

    latest_history = (
        history_events.withColumn(
            "_row_number",
            F.row_number().over(latest_history_window),
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop(
            "_row_number"
        )
        .select(
            "product_id",
            F.col("price").alias("history_price"),
            F.col("effective_from").alias(
                "history_effective_from"
            ),
        )
    )

    # =========================================================
    # 9. Add the latest valid product snapshot when:
    #
    # - no price history exists
    # - snapshot price differs from latest historical price
    # =========================================================

    snapshot_events = (
        valid_products.join(
            latest_history,
            on="product_id",
            how="left",
        )
        .filter(
            F.col("history_price").isNull()
            | (
                F.col("snapshot_price")
                != F.col("history_price")
            )
        )
        .select(
            "product_id",
            F.col("snapshot_price").alias("price"),
            F.col("snapshot_timestamp").alias(
                "effective_from"
            ),
            F.col("snapshot_timestamp").alias(
                "source_kafka_timestamp"
            ),
            F.lit("product_snapshot").alias(
                "source_kind"
            ),
        )
    )

    # =========================================================
    # 10. Combine history and snapshot events
    # =========================================================

    events = history_events.unionByName(
        snapshot_events
    )

    # =========================================================
    # 11. Resolve multiple events at the same timestamp
    #
    # A product snapshot wins over price history because it
    # represents the latest product state.
    # =========================================================

    events = events.withColumn(
        "_source_priority",
        F.when(
            F.col("source_kind") == "product_snapshot",
            F.lit(2),
        ).otherwise(
            F.lit(1)
        ),
    )

    same_time_window = Window.partitionBy(
        "product_id",
        "effective_from",
    ).orderBy(
        F.col("_source_priority").desc(),
        F.col("source_kafka_timestamp").desc_nulls_last(),
    )

    events = (
        events.withColumn(
            "_row_number",
            F.row_number().over(same_time_window),
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop(
            "_row_number",
            "_source_priority",
        )
    )

    # =========================================================
    # 12. Remove consecutive duplicate prices
    #
    # Example:
    #     100 -> 100 -> 120
    #
    # Result:
    #     100 -> 120
    # =========================================================

    change_window = Window.partitionBy(
        "product_id"
    ).orderBy(
        F.col("effective_from"),
        F.col("source_kafka_timestamp"),
    )

    events = (
        events.withColumn(
            "_previous_price",
            F.lag("price").over(change_window),
        )
        .filter(
            F.col("_previous_price").isNull()
            | (
                F.col("price")
                != F.col("_previous_price")
            )
        )
        .drop(
            "_previous_price"
        )
    )

    # =========================================================
    # 13. Build SCD2 validity intervals
    # =========================================================

    scd_window = Window.partitionBy(
        "product_id"
    ).orderBy(
        F.col("effective_from"),
        F.col("source_kafka_timestamp"),
    )

    scd_df = (
        events.withColumn(
            "effective_to",
            F.lead("effective_from").over(scd_window),
        )
        .withColumn(
            "is_current",
            F.col("effective_to").isNull(),
        )
    )

    # =========================================================
    # 14. Add descriptive product attributes
    # =========================================================

    product_attributes = valid_products.select(
        "product_id",
        "product_name",
    )

    scd_df = scd_df.join(
        product_attributes,
        on="product_id",
        how="left",
    )

    # =========================================================
    # 15. Build surrogate key and record hash
    # =========================================================

    valid_df = (
        scd_df.withColumn(
            "product_sk",
            F.xxhash64(
                F.concat_ws(
                    "||",
                    F.col("product_id"),
                    F.col("effective_from").cast("string"),
                )
            ),
        )
        .withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("product_name"),
                    F.col("price").cast("string"),
                ),
                256,
            ),
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
        )
    )

    # =========================================================
    # 16. Combine all invalid product records
    # =========================================================

    invalid_df = invalid_products.unionByName(
        invalid_history,
        allowMissingColumns=True,
    )

    return (
        valid_df,
        invalid_df,
    )
