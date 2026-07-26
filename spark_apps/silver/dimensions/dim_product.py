from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

from spark_apps.silver.config.tables import (
    TOPIC_PRODUCT_PRICE_HISTORY,
    TOPIC_PRODUCTS,
)


PRICE_TYPE = "decimal(10,2)"
UNKNOWN_PRODUCT_SK = 0
UNKNOWN_PRODUCT_ID = "__UNKNOWN__"


def add_unknown_product_member(source_df: DataFrame) -> DataFrame:
    """Add the system member referenced by unresolved order items."""
    unknown_df = source_df.sparkSession.range(1).select(
        F.lit(UNKNOWN_PRODUCT_SK).cast("long").alias("product_sk"),
        F.lit(UNKNOWN_PRODUCT_ID).alias("product_id"),
        F.lit("Unknown Product").alias("product_name"),
        F.lit(None).cast(PRICE_TYPE).alias("price"),
        F.to_timestamp(F.lit("1970-01-01 00:00:00")).alias("effective_from"),
        F.lit(None).cast("timestamp").alias("effective_to"),
        F.lit(True).alias("is_current"),
        F.sha2(F.lit("unknown_product"), 256).alias("record_hash"),
        F.lit("system").alias("source_kind"),
        F.lit(None).cast("timestamp").alias("source_kafka_timestamp"),
    )

    return source_df.unionByName(unknown_df)


def build_dim_product_source(
    products_df: DataFrame,
    price_history_df: DataFrame,
    *,
    persist_classified: bool = False,
) -> tuple[DataFrame, DataFrame]:
    """
    Build canonical Product SCD Type 2 source.

    Returns:
        valid_df:
            Canonical SCD2 product records.

        invalid_df:
            Invalid records prepared for quarantine.
    """

    # -----------------------------------------------------
    # Normalize product snapshots
    # -----------------------------------------------------

    normalized_products = (
        products_df.withColumn(
            "product_id",
            F.trim(F.col("product_id").cast("string")),
        )
        .withColumn(
            "product_name",
            F.trim(F.col("name").cast("string")),
        )
        .withColumn(
            "snapshot_price",
            F.col("price").cast(PRICE_TYPE),
        )
        .withColumn(
            "snapshot_timestamp",
            F.col("kafka_timestamp").cast("timestamp"),
        )
    )

    identified_products = normalized_products.filter(
        F.col("product_id").isNotNull() & (F.length(F.col("product_id")) > 0)
    )

    unidentified_products = normalized_products.filter(
        F.col("product_id").isNull() | (F.length(F.col("product_id")) == 0)
    )

    product_window = Window.partitionBy("product_id").orderBy(
        F.col("kafka_timestamp").desc_nulls_last(),
        F.col("kafka_partition").desc_nulls_last(),
        F.col("kafka_offset").desc_nulls_last(),
    )

    latest_products = (
        identified_products.withColumn(
            "_row_number",
            F.row_number().over(product_window),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
        .unionByName(
            unidentified_products,
            allowMissingColumns=True,
        )
    )

    validated_products = latest_products.withColumn(
        "_dq_error_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("product_id").isNull()
                | (F.length(F.col("product_id")) == 0),
                "missing_product_id",
            ),
            F.when(
                F.col("product_name").isNull()
                | (F.length(F.col("product_name")) == 0),
                "missing_product_name",
            ),
            F.when(
                F.col("snapshot_price").isNull(),
                "missing_product_price",
            ),
            F.when(
                F.col("snapshot_price") < 0,
                "negative_product_price",
            ),
            F.when(
                F.col("snapshot_timestamp").isNull(),
                "missing_product_timestamp",
            ),
        ),
    )

    if persist_classified:
        validated_products = validated_products.persist(StorageLevel.MEMORY_AND_DISK)

    valid_products = validated_products.filter(F.col("_dq_error_reason") == "")

    invalid_products = (
        validated_products.filter(
            F.col("_dq_error_reason") != ""
        )
        .withColumn(
            "_dq_entity",
            F.lit("product_snapshot"),
        )
        .withColumn(
            "_dq_source_topic",
            F.lit(TOPIC_PRODUCTS),
        )
    )

    # -----------------------------------------------------
    # Normalize price history
    # -----------------------------------------------------

    normalized_history = (
        price_history_df.withColumn(
            "product_id",
            F.trim(F.col("product_id").cast("string")),
        )
        .withColumn(
            "price",
            F.col("price").cast(PRICE_TYPE),
        )
        .withColumn(
            "effective_from",
            F.col("valid_from").cast("timestamp"),
        )
        .withColumn(
            "source_kafka_timestamp",
            F.col("kafka_timestamp").cast("timestamp"),
        )
    )

    validated_history = normalized_history.withColumn(
        "_dq_error_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("product_id").isNull()
                | (F.length(F.col("product_id")) == 0),
                "missing_product_id",
            ),
            F.when(
                F.col("price").isNull(),
                "missing_price",
            ),
            F.when(
                F.col("price") < 0,
                "negative_price",
            ),
            F.when(
                F.col("effective_from").isNull(),
                "missing_valid_from",
            ),
            F.when(
                F.col("source_kafka_timestamp").isNull(),
                "missing_kafka_timestamp",
            ),
        ),
    )

    if persist_classified:
        validated_history = validated_history.persist(StorageLevel.MEMORY_AND_DISK)

    valid_history = validated_history.filter(
        F.col("_dq_error_reason") == ""
    )

    invalid_history = (
        validated_history.filter(
            F.col("_dq_error_reason") != ""
        )
        .withColumn(
            "_dq_entity",
            F.lit("product_price_history"),
        )
        .withColumn(
            "_dq_source_topic",
            F.lit(TOPIC_PRODUCT_PRICE_HISTORY),
        )
    )

    # -----------------------------------------------------
    # Build SCD2 events
    # -----------------------------------------------------

    history_events = valid_history.select(
        "product_id",
        "price",
        "effective_from",
        "source_kafka_timestamp",
        F.lit("price_history").alias("source_kind"),
    )

    latest_history = (
        history_events.withColumn(
            "_row_number",
            F.row_number().over(
                Window.partitionBy(
                    "product_id"
                ).orderBy(
                    F.col("effective_from").desc_nulls_last(),
                    F.col("source_kafka_timestamp").desc_nulls_last(),
                )
            ),
        )
        .filter(F.col("_row_number") == 1)
        .select(
            "product_id",
            F.col("price").alias("history_price"),
        )
    )

    snapshot_events = (
        valid_products.join(
            latest_history,
            "product_id",
            "left",
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

    events = history_events.unionByName(
        snapshot_events
    )

    same_time_window = Window.partitionBy(
        "product_id",
        "effective_from",
    ).orderBy(
        F.when(
            F.col("source_kind") == "product_snapshot",
            2,
        )
        .otherwise(1)
        .desc(),
        F.col("source_kafka_timestamp").desc_nulls_last(),
    )

    events = (
        events.withColumn(
            "_row_number",
            F.row_number().over(same_time_window),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    change_window = Window.partitionBy(
        "product_id"
    ).orderBy(
        "effective_from",
        "source_kafka_timestamp",
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
        .drop("_previous_price")
    )

    # -----------------------------------------------------
    # Create SCD2 intervals
    # -----------------------------------------------------

    scd_window = Window.partitionBy(
        "product_id"
    ).orderBy(
        "effective_from",
        "source_kafka_timestamp",
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

    product_attributes = valid_products.select(
        "product_id",
        "product_name",
    )

    valid_df = (
        scd_df.join(
            product_attributes,
            "product_id",
            "left",
        )
        .withColumn(
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

    invalid_df = invalid_products.unionByName(
        invalid_history,
        allowMissingColumns=True,
    )

    return valid_df, invalid_df