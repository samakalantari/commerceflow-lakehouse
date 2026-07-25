from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_fact_return_refund_source(
    returns_df: DataFrame,
    fact_order_item_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Build one canonical row per return_refund_id."""
    normalized = (
        returns_df.withColumn(
            "return_refund_id",
            F.trim(F.col("return_refund_id").cast("string")),
        )
        .withColumn("order_id", F.trim(F.col("order_id").cast("string")))
        .withColumn(
            "order_item_id",
            F.trim(F.col("order_item_id").cast("string")),
        )
        .withColumn(
            "return_reason",
            F.lower(F.trim(F.col("return_reason").cast("string"))),
        )
        .withColumn(
            "return_timestamp",
            F.col("return_timestamp").cast("timestamp"),
        )
        .withColumn(
            "refund_amount",
            F.col("refund_amount").cast("decimal(10,2)"),
        )
    )

    identified = normalized.filter(
        F.col("return_refund_id").isNotNull()
        & (F.length(F.col("return_refund_id")) > 0)
    )
    unidentified = normalized.filter(
        F.col("return_refund_id").isNull()
        | (F.length(F.col("return_refund_id")) == 0)
    )

    latest_window = Window.partitionBy("return_refund_id").orderBy(
        F.col("kafka_timestamp").desc_nulls_last(),
        F.col("kafka_partition").desc_nulls_last(),
        F.col("kafka_offset").desc_nulls_last(),
    )

    latest = (
        identified.withColumn("_row_number", F.row_number().over(latest_window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
        .unionByName(unidentified, allowMissingColumns=True)
    )

    validated = latest.withColumn(
        "_dq_error_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("return_refund_id").isNull()
                | (F.length(F.col("return_refund_id")) == 0),
                "missing_return_refund_id",
            ),
            F.when(
                F.col("order_id").isNull() | (F.length(F.col("order_id")) == 0),
                "missing_order_id",
            ),
            F.when(
                F.col("order_item_id").isNull()
                | (F.length(F.col("order_item_id")) == 0),
                "missing_order_item_id",
            ),
            F.when(F.col("return_timestamp").isNull(), "missing_return_timestamp"),
            F.when(F.col("refund_amount").isNull(), "missing_refund_amount"),
            F.when(F.col("refund_amount") < 0, "negative_refund_amount"),
            F.when(
                F.col("return_reason").isNull()
                | (F.length(F.col("return_reason")) == 0),
                "missing_return_reason",
            ),
            F.when(F.col("kafka_timestamp").isNull(), "missing_kafka_timestamp"),
        ),
    )

    base_valid = validated.filter(F.col("_dq_error_reason") == "")
    base_invalid = (
        validated.filter(F.col("_dq_error_reason") != "")
        .withColumn("_dq_entity", F.lit("return_refund"))
        .withColumn("_dq_source_topic", F.lit("transactional.returns_refunds"))
    )

    resolved = (
        base_valid.alias("refund")
        .join(
            fact_order_item_df.select(
                "order_item_id",
                "order_item_sk",
                "order_id",
                "order_sk",
                "order_timestamp",
            ).alias("item"),
            F.col("refund.order_item_id") == F.col("item.order_item_id"),
            "left",
        )
        .select(
            "refund.*",
            F.col("item.order_item_sk").alias("resolved_order_item_sk"),
            F.col("item.order_sk").alias("resolved_order_sk"),
            F.col("item.order_id").alias("resolved_order_id"),
            F.col("item.order_timestamp").alias("resolved_order_timestamp"),
        )
        .withColumn(
            "_dq_error_reason",
            F.concat_ws(
                "; ",
                F.when(
                    F.col("resolved_order_item_sk").isNull(),
                    "missing_parent_order_item",
                ),
                F.when(
                    F.col("resolved_order_item_sk").isNotNull()
                    & (F.col("order_id") != F.col("resolved_order_id")),
                    "order_item_order_mismatch",
                ),
                F.when(
                    F.col("resolved_order_timestamp").isNotNull()
                    & (F.col("return_timestamp") < F.col("resolved_order_timestamp")),
                    "return_before_order",
                ),
            ),
        )
    )

    relationship_invalid = (
        resolved.filter(F.col("_dq_error_reason") != "")
        .withColumn("_dq_entity", F.lit("return_refund"))
        .withColumn("_dq_source_topic", F.lit("transactional.returns_refunds"))
    )

    valid_df = resolved.filter(F.col("_dq_error_reason") == "").select(
        F.xxhash64("return_refund_id").alias("return_refund_sk"),
        "return_refund_id",
        F.col("resolved_order_sk").alias("order_sk"),
        "order_id",
        F.col("resolved_order_item_sk").alias("order_item_sk"),
        "order_item_id",
        F.date_format("return_timestamp", "yyyyMMdd").cast("int").alias("return_date_sk"),
        "return_timestamp",
        "refund_amount",
        "return_reason",
        F.col("kafka_timestamp").alias("source_kafka_timestamp"),
    )

    invalid_df = base_invalid.unionByName(
        relationship_invalid,
        allowMissingColumns=True,
    )

    return valid_df, invalid_df
