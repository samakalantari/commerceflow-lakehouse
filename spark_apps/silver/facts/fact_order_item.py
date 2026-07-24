from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


UNKNOWN_PRODUCT_SK = 0


def build_fact_order_item_source(
    items_df: DataFrame,
    fact_order_df: DataFrame,
    dim_product_df: DataFrame,
) -> DataFrame:
    """
    Build the canonical fact_order_item source.

    Grain:
        One row per order_item_id.

    Product resolution:
        1. Match the product SCD2 version valid at order_timestamp.
        2. Use UNKNOWN_PRODUCT_SK when no temporal version is found.

    Notes:
        Because dim_product does not contain an Unknown Product row,
        downstream consumers must use a left join when joining this fact
        to dim_product.
    """

    # ---------------------------------------------------------
    # 1. Clean and normalize source order items
    # ---------------------------------------------------------

    normalized_items = (
        items_df.filter(F.col("order_item_id").isNotNull())
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") >= 0)
        .withColumn(
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
        .filter(F.length(F.col("order_item_id")) > 0)
        .filter(F.length(F.col("order_id")) > 0)
        .filter(F.length(F.col("product_id")) > 0)
    )

    # ---------------------------------------------------------
    # 2. Deduplicate order items
    #
    # Keep the latest Kafka version per order_item_id.
    # ---------------------------------------------------------

    latest_item_window = Window.partitionBy("order_item_id").orderBy(
        F.col("kafka_timestamp").desc(),
        F.col("kafka_partition").desc(),
        F.col("kafka_offset").desc(),
    )

    items = (
        normalized_items.withColumn(
            "_row_number",
            F.row_number().over(latest_item_window),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
        .select(
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "item_total_amount",
            F.col("kafka_timestamp").alias("source_kafka_timestamp"),
        )
    )

    # ---------------------------------------------------------
    # 3. Attach order information
    # ---------------------------------------------------------

    items_with_order = (
        items.alias("i")
        .join(
            fact_order_df.alias("o"),
            F.col("i.order_id") == F.col("o.order_id"),
            how="left",
        )
        .select(
            F.col("i.order_item_id").alias("order_item_id"),
            F.col("i.order_id").alias("order_id"),
            F.col("i.product_id").alias("product_id"),
            F.col("i.quantity").alias("quantity"),
            F.col("i.unit_price").alias("unit_price"),
            F.col("i.item_total_amount").alias("item_total_amount"),
            F.col("i.source_kafka_timestamp").alias("source_kafka_timestamp"),
            F.col("o.order_sk").alias("order_sk"),
            F.col("o.order_date_sk").alias("order_date_sk"),
            F.col("o.order_timestamp").alias("order_timestamp"),
        )
    )

    # ---------------------------------------------------------
    # 4. Temporal SCD2 product lookup
    # ---------------------------------------------------------

    temporal_matches = (
        items_with_order.alias("i")
        .join(
            dim_product_df.alias("p"),
            (F.col("i.product_id") == F.col("p.product_id"))
            & (F.col("i.order_timestamp") >= F.col("p.effective_from"))
            & (
                F.col("p.effective_to").isNull()
                | (F.col("i.order_timestamp") < F.col("p.effective_to"))
            ),
            how="left",
        )
        .select(
            F.col("i.order_item_id").alias("order_item_id"),
            F.col("i.order_id").alias("order_id"),
            F.col("i.product_id").alias("product_id"),
            F.col("i.quantity").alias("quantity"),
            F.col("i.unit_price").alias("unit_price"),
            F.col("i.item_total_amount").alias("item_total_amount"),
            F.col("i.source_kafka_timestamp").alias("source_kafka_timestamp"),
            F.col("i.order_sk").alias("order_sk"),
            F.col("i.order_date_sk").alias("order_date_sk"),
            F.col("i.order_timestamp").alias("order_timestamp"),
            F.col("p.product_sk").alias("temporal_product_sk"),
        )
    )

    # ---------------------------------------------------------
    # 5. Resolve final product surrogate key
    # ---------------------------------------------------------

    resolved = (
        temporal_matches
        .withColumn(
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

    # ---------------------------------------------------------
    # 6. Build final fact structure
    # ---------------------------------------------------------

    return resolved.select(
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