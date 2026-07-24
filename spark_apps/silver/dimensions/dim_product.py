from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_dim_product_source(
    products_df: DataFrame,
    price_history_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Build canonical Product SCD Type 2 source.

    Price history is the historical source.

    The latest product snapshot is added when:
      - the product has no price history
      - the latest snapshot price differs from
        the latest price-history price
    """
    
    # ---------------------------------------------------------
    # 1. Normalize product snapshots
    # ---------------------------------------------------------

    normalized_products = (
        products_df
        .withColumn(
            "product_id",
            F.trim(F.col("product_id")),
        )
        .withColumn(
            "product_name",
            F.trim(F.col("name")),
        )
    )

    # ---------------------------------------------------------
    # 2. Keep latest product snapshot
    # ---------------------------------------------------------

    product_window = Window.partitionBy("product_id").orderBy(
        F.col("kafka_timestamp").desc_nulls_last(),
        F.col("kafka_partition").desc_nulls_last(),
        F.col("kafka_offset").desc_nulls_last(),
    )

    latest_products = (
        normalized_products
        .withColumn(
            "_rn",
            F.row_number().over(product_window),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            "product_id",
            "product_name",
            F.col("price").alias("snapshot_price"),
            F.col("kafka_timestamp").alias("snapshot_timestamp"),
            "kafka_partition",
            "kafka_offset",
        )
    )

    # ---------------------------------------------------------
    # 2. Clean price-history events
    # ---------------------------------------------------------

    history_events = (
        price_history_df.filter(F.col("product_id").isNotNull())
        .filter(F.col("price").isNotNull())
        .filter(F.col("price") >= 0)
        .filter(F.col("valid_from").isNotNull())
        .select(
            F.trim(F.col("product_id")).alias("product_id"),
            F.col("price"),
            F.col("valid_from").alias("effective_from"),
            F.col("kafka_timestamp").alias("source_kafka_timestamp"),
            F.lit("price_history").alias("source_kind"),
        )
    )

    # ---------------------------------------------------------
    # 3. Find latest historical price
    # ---------------------------------------------------------

    latest_history_window = Window.partitionBy("product_id").orderBy(
        F.col("effective_from").desc(),
        F.col("source_kafka_timestamp").desc(),
    )

    latest_history = (
        history_events.withColumn(
            "_rn",
            F.row_number().over(latest_history_window),
        )
        .filter(F.col("_rn") == 1)
        .select(
            "product_id",
            F.col("price").alias("history_price"),
            F.col("effective_from").alias("history_effective_from"),
        )
    )

    # ---------------------------------------------------------
    # 4. Add product snapshot when history is missing
    #    or current price differs
    # ---------------------------------------------------------

    snapshot_events = (
        latest_products.join(
            latest_history,
            on="product_id",
            how="left",
        )
        .filter(
            F.col("history_price").isNull() | (F.col("snapshot_price") != F.col("history_price"))
        )
        .select(
            "product_id",
            F.col("snapshot_price").alias("price"),
            F.col("snapshot_timestamp").alias("effective_from"),
            F.col("snapshot_timestamp").alias("source_kafka_timestamp"),
            F.lit("product_snapshot").alias("source_kind"),
        )
    )

    # ---------------------------------------------------------
    # 5. Combine history + current snapshot corrections
    # ---------------------------------------------------------

    events = history_events.unionByName(snapshot_events)

    # ---------------------------------------------------------
    # 6. Resolve multiple events at exact same timestamp
    #
    # Snapshot wins because it represents current product state.
    # ---------------------------------------------------------

    events = events.withColumn(
        "_source_priority",
        F.when(
            F.col("source_kind") == "product_snapshot",
            2,
        ).otherwise(1),
    )

    same_time_window = Window.partitionBy(
        "product_id",
        "effective_from",
    ).orderBy(
        F.col("_source_priority").desc(),
        F.col("source_kafka_timestamp").desc(),
    )

    events = (
        events.withColumn(
            "_rn",
            F.row_number().over(same_time_window),
        )
        .filter(F.col("_rn") == 1)
        .drop(
            "_rn",
            "_source_priority",
        )
    )

    # ---------------------------------------------------------
    # 7. Remove consecutive duplicate prices
    #
    # Example:
    # 100 -> 100 -> 120
    #
    # becomes:
    # 100 -> 120
    # ---------------------------------------------------------

    change_window = Window.partitionBy("product_id").orderBy(
        F.col("effective_from"),
        F.col("source_kafka_timestamp"),
    )

    events = (
        events.withColumn(
            "_previous_price",
            F.lag("price").over(change_window),
        )
        .filter(F.col("_previous_price").isNull() | (F.col("price") != F.col("_previous_price")))
        .drop("_previous_price")
    )

    # ---------------------------------------------------------
    # 8. Build SCD2 validity intervals
    # ---------------------------------------------------------

    scd_window = Window.partitionBy("product_id").orderBy(
        F.col("effective_from"),
        F.col("source_kafka_timestamp"),
    )

    scd_df = events.withColumn(
        "effective_to",
        F.lead("effective_from").over(scd_window),
    ).withColumn(
        "is_current",
        F.col("effective_to").isNull(),
    )

    # ---------------------------------------------------------
    # 9. Add descriptive product attributes
    # ---------------------------------------------------------

    scd_df = scd_df.join(
        latest_products.select(
            "product_id",
            "product_name",
        ),
        on="product_id",
        how="left",
    )

    # ---------------------------------------------------------
    # 10. Surrogate key + record hash
    # ---------------------------------------------------------

    return (
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
