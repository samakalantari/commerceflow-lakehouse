from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

UNKNOWN_PRODUCT_SK = 0
AMOUNT_TOLERANCE = 0.01


def build_fact_order_item_source(
    items_df: DataFrame,
    fact_order_df: DataFrame,
    dim_product_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Build the canonical fact_order_item source.

    Grain:
        One row per order_item_id.

    Rules:
        - Normalize business keys before deduplication.
        - Preserve records without order_item_id for validation.
        - Keep the latest Kafka version of each identified item.
        - Validate quantities, prices and item totals.
        - Require a valid parent order.
        - Resolve the product version valid at order_timestamp.
        - Use UNKNOWN_PRODUCT_SK when no temporal product
          version is available.

    Returns:
        valid_df:
            Valid and dimension-resolved fact_order_item rows.

        invalid_df:
            Invalid source rows with data-quality error reasons.
    """

    # =========================================================
    # 1. Normalize source order items
    # =========================================================

    normalized_items = (
        items_df.withColumn(
            "order_item_id",
            F.trim(F.col("order_item_id")),
        )
        .withColumn(
            "order_id",
            F.trim(F.col("order_id")),
        )
        .withColumn(
            "product_id",
            F.trim(F.col("product_id")),
        )
    )

    # =========================================================
    # 2. Separate rows without a usable order_item_id
    #
    # Missing business keys must not be deduplicated together.
    # Every invalid source message must remain available.
    # =========================================================

    identified_items = normalized_items.filter(
        F.col("order_item_id").isNotNull()
        & (F.length(F.col("order_item_id")) > 0)
    )

    unidentified_items = normalized_items.filter(
        F.col("order_item_id").isNull()
        | (F.length(F.col("order_item_id")) == 0)
    )

    # =========================================================
    # 3. Keep the latest Kafka version per identified item
    # =========================================================

    latest_item_window = Window.partitionBy(
        "order_item_id"
    ).orderBy(
        F.col("kafka_timestamp").desc_nulls_last(),
        F.col("kafka_partition").desc_nulls_last(),
        F.col("kafka_offset").desc_nulls_last(),
    )

    latest_identified_items = (
        identified_items.withColumn(
            "_row_number",
            F.row_number().over(latest_item_window),
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop(
            "_row_number"
        )
    )

    latest_items = latest_identified_items.unionByName(
        unidentified_items,
        allowMissingColumns=True,
    )

    # =========================================================
    # 4. Validate source order-item fields
    # =========================================================

    validated_items = latest_items.withColumn(
        "_dq_error_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("order_item_id").isNull()
                | (F.length(F.col("order_item_id")) == 0),
                F.lit("missing_order_item_id"),
            ),
            F.when(
                F.col("order_id").isNull()
                | (F.length(F.col("order_id")) == 0),
                F.lit("missing_order_id"),
            ),
            F.when(
                F.col("product_id").isNull()
                | (F.length(F.col("product_id")) == 0),
                F.lit("missing_product_id"),
            ),
            F.when(
                F.col("quantity").isNull(),
                F.lit("missing_quantity"),
            ),
            F.when(
                F.col("quantity").isNotNull()
                & (F.col("quantity") <= 0),
                F.lit("non_positive_quantity"),
            ),
            F.when(
                F.col("unit_price").isNull(),
                F.lit("missing_unit_price"),
            ),
            F.when(
                F.col("unit_price").isNotNull()
                & (F.col("unit_price") < 0),
                F.lit("negative_unit_price"),
            ),
            F.when(
                F.col("item_total_amount").isNull(),
                F.lit("missing_item_total_amount"),
            ),
            F.when(
                F.col("item_total_amount").isNotNull()
                & (F.col("item_total_amount") < 0),
                F.lit("negative_item_total_amount"),
            ),
            F.when(
                F.col("quantity").isNotNull()
                & F.col("unit_price").isNotNull()
                & F.col("item_total_amount").isNotNull()
                & (
                    F.abs(
                        F.col("item_total_amount")
                        - (
                            F.col("quantity")
                            * F.col("unit_price")
                        )
                    )
                    > F.lit(AMOUNT_TOLERANCE)
                ),
                F.lit("item_total_mismatch"),
            ),
            F.when(
                F.col("kafka_timestamp").isNull(),
                F.lit("missing_kafka_timestamp"),
            ),
        ),
    )

    base_valid_items = validated_items.filter(
        F.col("_dq_error_reason") == ""
    )

    base_invalid_items = (
        validated_items.filter(
            F.col("_dq_error_reason") != ""
        )
        .withColumn(
            "_dq_source_entity",
            F.lit("order_item"),
        )
    )

    # =========================================================
    # 5. Prepare valid source fields
    # =========================================================

    canonical_items = base_valid_items.select(
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "item_total_amount",
        F.col("kafka_timestamp").alias(
            "source_kafka_timestamp"
        ),
        "kafka_partition",
        "kafka_offset",
    )

    # =========================================================
    # 6. Attach parent order information
    # =========================================================

    items_with_order = (
        canonical_items.alias("item")
        .join(
            fact_order_df.select(
                "order_id",
                "order_sk",
                "order_date_sk",
                "order_timestamp",
            ).alias("order"),
            F.col("item.order_id")
            == F.col("order.order_id"),
            how="left",
        )
        .select(
            F.col("item.order_item_id").alias(
                "order_item_id"
            ),
            F.col("item.order_id").alias(
                "order_id"
            ),
            F.col("item.product_id").alias(
                "product_id"
            ),
            F.col("item.quantity").alias(
                "quantity"
            ),
            F.col("item.unit_price").alias(
                "unit_price"
            ),
            F.col("item.item_total_amount").alias(
                "item_total_amount"
            ),
            F.col("item.source_kafka_timestamp").alias(
                "source_kafka_timestamp"
            ),
            F.col("item.kafka_partition").alias(
                "kafka_partition"
            ),
            F.col("item.kafka_offset").alias(
                "kafka_offset"
            ),
            F.col("order.order_sk").alias(
                "order_sk"
            ),
            F.col("order.order_date_sk").alias(
                "order_date_sk"
            ),
            F.col("order.order_timestamp").alias(
                "order_timestamp"
            ),
        )
    )

    # =========================================================
    # 7. Validate parent-order relationship
    # =========================================================

    validated_order_relationship = items_with_order.withColumn(
        "_dq_error_reason",
        F.when(
            F.col("order_sk").isNull(),
            F.lit("missing_parent_order"),
        ).otherwise(
            F.lit("")
        ),
    )

    valid_items_with_order = validated_order_relationship.filter(
        F.col("_dq_error_reason") == ""
    )

    orphan_items = (
        validated_order_relationship.filter(
            F.col("_dq_error_reason") != ""
        )
        .withColumn(
            "_dq_source_entity",
            F.lit("order_item"),
        )
    )

    # =========================================================
    # 8. Resolve temporal Product SCD2 version
    # =========================================================

    temporal_matches = (
        valid_items_with_order.alias("item")
        .join(
            dim_product_df.select(
                "product_id",
                "product_sk",
                "effective_from",
                "effective_to",
            ).alias("product"),
            (
                F.col("item.product_id")
                == F.col("product.product_id")
            )
            & (
                F.col("item.order_timestamp")
                >= F.col("product.effective_from")
            )
            & (
                F.col("product.effective_to").isNull()
                | (
                    F.col("item.order_timestamp")
                    < F.col("product.effective_to")
                )
            ),
            how="left",
        )
        .select(
            F.col("item.order_item_id").alias(
                "order_item_id"
            ),
            F.col("item.order_id").alias(
                "order_id"
            ),
            F.col("item.product_id").alias(
                "product_id"
            ),
            F.col("item.quantity").alias(
                "quantity"
            ),
            F.col("item.unit_price").alias(
                "unit_price"
            ),
            F.col("item.item_total_amount").alias(
                "item_total_amount"
            ),
            F.col("item.source_kafka_timestamp").alias(
                "source_kafka_timestamp"
            ),
            F.col("item.order_sk").alias(
                "order_sk"
            ),
            F.col("item.order_date_sk").alias(
                "order_date_sk"
            ),
            F.col("item.order_timestamp").alias(
                "order_timestamp"
            ),
            F.col("product.product_sk").alias(
                "temporal_product_sk"
            ),
        )
    )

    # =========================================================
    # 9. Resolve final product surrogate key
    #
    # An unresolved product is not treated as an invalid source
    # row. It is mapped to the Unknown Product member.
    # =========================================================

    resolved_items = (
        temporal_matches.withColumn(
            "product_sk",
            F.coalesce(
                F.col("temporal_product_sk"),
                F.lit(UNKNOWN_PRODUCT_SK).cast("long"),
            ),
        )
        .withColumn(
            "product_resolution",
            F.when(
                F.col("temporal_product_sk").isNotNull(),
                F.lit("temporal"),
            ).otherwise(
                F.lit("unknown_product"),
            ),
        )
    )

    # =========================================================
    # 10. Build final valid fact structure
    # =========================================================

    valid_df = resolved_items.select(
        F.xxhash64(
            F.col("order_item_id")
        ).alias(
            "order_item_sk"
        ),
        F.col("order_item_id"),
        F.col("order_sk"),
        F.col("order_id"),
        F.col("product_sk"),
        F.col("product_id"),
        F.col("order_date_sk"),
        F.col("order_timestamp"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("item_total_amount"),
        F.col("product_resolution"),
        F.col("source_kafka_timestamp"),
    )

    # =========================================================
    # 11. Combine all invalid order-item records
    # =========================================================

    invalid_df = base_invalid_items.unionByName(
        orphan_items,
        allowMissingColumns=True,
    )

    return (
        valid_df,
        invalid_df,
    )
