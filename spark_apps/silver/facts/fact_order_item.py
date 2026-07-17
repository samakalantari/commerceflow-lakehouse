from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_fact_order_item_source(
    items_df: DataFrame,
    fact_order_df: DataFrame,
    dim_product_df: DataFrame,
) -> DataFrame:
    """
    Build fact_order_item.

    Grain:
        One row per order_item_id.

    Product resolution:
        1. Temporal SCD2 match at order_timestamp
        2. Earliest known product version as fallback
    """

    # ---------------------------------------------------------
    # 1. Clean source order items
    # ---------------------------------------------------------

    items = (
        items_df
        .filter(
            F.col("order_item_id").isNotNull()
        )
        .filter(
            F.col("order_id").isNotNull()
        )
        .filter(
            F.col("product_id").isNotNull()
        )
        .filter(
            F.col("quantity") > 0
        )
        .filter(
            F.col("unit_price") >= 0
        )
        .select(
            F.trim(
                F.col("order_item_id")
            ).alias(
                "order_item_id"
            ),

            F.trim(
                F.col("order_id")
            ).alias(
                "order_id"
            ),

            F.trim(
                F.col("product_id")
            ).alias(
                "product_id"
            ),

            F.col(
                "quantity"
            ),

            F.col(
                "unit_price"
            ),

            F.col(
                "item_total_amount"
            ),

            F.col(
                "kafka_timestamp"
            ).alias(
                "source_kafka_timestamp"
            ),
        )
    )

    # ---------------------------------------------------------
    # 2. Attach order information
    # ---------------------------------------------------------

    items_with_order = (
        items.alias("i")
        .join(
            fact_order_df.alias("o"),
            F.col("i.order_id")
            ==
            F.col("o.order_id"),
            how="left",
        )
        .select(
            F.col(
                "i.order_item_id"
            ).alias(
                "order_item_id"
            ),

            F.col(
                "i.order_id"
            ).alias(
                "order_id"
            ),

            F.col(
                "i.product_id"
            ).alias(
                "product_id"
            ),

            F.col(
                "i.quantity"
            ).alias(
                "quantity"
            ),

            F.col(
                "i.unit_price"
            ).alias(
                "unit_price"
            ),

            F.col(
                "i.item_total_amount"
            ).alias(
                "item_total_amount"
            ),

            F.col(
                "i.source_kafka_timestamp"
            ).alias(
                "source_kafka_timestamp"
            ),

            F.col(
                "o.order_sk"
            ).alias(
                "order_sk"
            ),

            F.col(
                "o.order_date_sk"
            ).alias(
                "order_date_sk"
            ),

            F.col(
                "o.order_timestamp"
            ).alias(
                "order_timestamp"
            ),
        )
    )

    # ---------------------------------------------------------
    # 3. Temporal SCD2 lookup
    # ---------------------------------------------------------

    temporal_matches = (
        items_with_order.alias("i")
        .join(
            dim_product_df.alias("p"),
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
                "i.order_id"
            ).alias(
                "order_id"
            ),

            F.col(
                "i.product_id"
            ).alias(
                "product_id"
            ),

            F.col(
                "i.quantity"
            ).alias(
                "quantity"
            ),

            F.col(
                "i.unit_price"
            ).alias(
                "unit_price"
            ),

            F.col(
                "i.item_total_amount"
            ).alias(
                "item_total_amount"
            ),

            F.col(
                "i.source_kafka_timestamp"
            ).alias(
                "source_kafka_timestamp"
            ),

            F.col(
                "i.order_sk"
            ).alias(
                "order_sk"
            ),

            F.col(
                "i.order_date_sk"
            ).alias(
                "order_date_sk"
            ),

            F.col(
                "i.order_timestamp"
            ).alias(
                "order_timestamp"
            ),

            F.col(
                "p.product_sk"
            ).alias(
                "temporal_product_sk"
            ),
        )
    )

    # ---------------------------------------------------------
    # 4. Earliest known product version
    #    Used only when temporal lookup fails
    # ---------------------------------------------------------

    earliest_window = (
        Window
        .partitionBy(
            "product_id"
        )
        .orderBy(
            F.col(
                "effective_from"
            ).asc()
        )
    )

    earliest_products = (
        dim_product_df
        .withColumn(
            "_rn",
            F.row_number().over(
                earliest_window
            ),
        )
        .filter(
            F.col("_rn") == 1
        )
        .select(
            F.col(
                "product_id"
            ).alias(
                "fallback_product_id"
            ),

            F.col(
                "product_sk"
            ).alias(
                "earliest_product_sk"
            ),
        )
    )

    # ---------------------------------------------------------
    # 5. Resolve final product surrogate key
    # ---------------------------------------------------------

    resolved = (
        temporal_matches.alias("i")
        .join(
            earliest_products.alias("p"),
            F.col(
                "i.product_id"
            )
            ==
            F.col(
                "p.fallback_product_id"
            ),
            how="left",
        )
        .withColumn(
            "resolved_product_sk",
            F.coalesce(
                F.col(
                    "i.temporal_product_sk"
                ),
                F.col(
                    "p.earliest_product_sk"
                ),
            ),
        )
        .withColumn(
            "product_resolution",
            F.when(
                F.col(
                    "i.temporal_product_sk"
                ).isNotNull(),
                F.lit(
                    "temporal"
                ),
            ).otherwise(
                F.lit(
                    "earliest_fallback"
                ),
            ),
        )
    )

    # ---------------------------------------------------------
    # 6. Final Fact structure
    # ---------------------------------------------------------

    result = (
        resolved
        .select(
            F.xxhash64(
                F.col(
                    "i.order_item_id"
                )
            ).alias(
                "order_item_sk"
            ),

            F.col(
                "i.order_item_id"
            ).alias(
                "order_item_id"
            ),

            F.col(
                "i.order_sk"
            ).alias(
                "order_sk"
            ),

            F.col(
                "i.order_id"
            ).alias(
                "order_id"
            ),

            F.col(
                "resolved_product_sk"
            ).alias(
                "product_sk"
            ),

            F.col(
                "i.product_id"
            ).alias(
                "product_id"
            ),

            F.col(
                "i.order_date_sk"
            ).alias(
                "order_date_sk"
            ),

            F.col(
                "i.order_timestamp"
            ).alias(
                "order_timestamp"
            ),

            F.col(
                "i.quantity"
            ).alias(
                "quantity"
            ),

            F.col(
                "i.unit_price"
            ).alias(
                "unit_price"
            ),

            F.col(
                "i.item_total_amount"
            ).alias(
                "item_total_amount"
            ),

            F.col(
                "product_resolution"
            ).alias(
                "product_resolution"
            ),

            F.col(
                "i.source_kafka_timestamp"
            ).alias(
                "source_kafka_timestamp"
            ),
        )
    )

    return result
