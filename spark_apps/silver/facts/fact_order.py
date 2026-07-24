from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_fact_order_source(
    orders_df: DataFrame,
    dim_user_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Build the canonical fact_order source.

    Grain:
        One row per order_id.

    Rules:
        - Normalize source fields before deduplication.
        - Preserve records without a usable order_id for validation.
        - Keep the latest Kafka version of each identified order.
        - Validate required fields and business values.
        - Resolve user_sk from dim_user.
        - Use Unknown User (-1) when a valid user_id is not
          available in dim_user.

    Returns:
        valid_df:
            Valid and dimension-resolved fact_order records.

        invalid_df:
            Invalid source order records with data-quality
            error reasons.
    """

    # =========================================================
    # 1. Normalize source orders
    # =========================================================

    normalized_orders = (
        orders_df.withColumn(
            "order_id",
            F.trim(F.col("order_id")),
        )
        .withColumn(
            "user_id",
            F.trim(F.col("user_id")),
        )
        .withColumn(
            "status",
            F.lower(F.trim(F.col("status"))),
        )
        .withColumn(
            "payment_method",
            F.lower(F.trim(F.col("payment_method"))),
        )
    )

    # =========================================================
    # 2. Separate orders without a usable business key
    #
    # Records without order_id must not be deduplicated
    # together because every NULL or empty order_id would
    # otherwise fall into the same window partition.
    # =========================================================

    identified_orders = normalized_orders.filter(
        F.col("order_id").isNotNull()
        & (F.length(F.col("order_id")) > 0)
    )

    unidentified_orders = normalized_orders.filter(
        F.col("order_id").isNull()
        | (F.length(F.col("order_id")) == 0)
    )

    # =========================================================
    # 3. Keep the latest Kafka version per identified order
    # =========================================================

    latest_order_window = Window.partitionBy(
        "order_id"
    ).orderBy(
        F.col("kafka_timestamp").desc_nulls_last(),
        F.col("kafka_partition").desc_nulls_last(),
        F.col("kafka_offset").desc_nulls_last(),
    )

    latest_identified_orders = (
        identified_orders.withColumn(
            "_row_number",
            F.row_number().over(latest_order_window),
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop(
            "_row_number"
        )
    )

    # Preserve every record without order_id so each invalid
    # source message remains available for later quarantine.
    latest_orders = latest_identified_orders.unionByName(
        unidentified_orders,
        allowMissingColumns=True,
    )

    # =========================================================
    # 4. Validate latest source orders
    # =========================================================

    validated_orders = latest_orders.withColumn(
        "_dq_error_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("order_id").isNull()
                | (F.length(F.col("order_id")) == 0),
                F.lit("missing_order_id"),
            ),
            F.when(
                F.col("user_id").isNull()
                | (F.length(F.col("user_id")) == 0),
                F.lit("missing_user_id"),
            ),
            F.when(
                F.col("timestamp").isNull(),
                F.lit("missing_order_timestamp"),
            ),
            F.when(
                F.col("timestamp").isNotNull()
                & (
                    F.col("timestamp")
                    > F.current_timestamp()
                ),
                F.lit("future_order_timestamp"),
            ),
            F.when(
                F.col("total").isNull(),
                F.lit("missing_order_total"),
            ),
            F.when(
                F.col("total").isNotNull()
                & (F.col("total") < 0),
                F.lit("negative_order_total"),
            ),
            F.when(
                F.col("status").isNull()
                | (F.length(F.col("status")) == 0),
                F.lit("missing_order_status"),
            ),
            F.when(
                F.col("payment_method").isNull()
                | (F.length(F.col("payment_method")) == 0),
                F.lit("missing_payment_method"),
            ),
            F.when(
                F.col("kafka_timestamp").isNull(),
                F.lit("missing_kafka_timestamp"),
            ),
        ),
    )

    # =========================================================
    # 5. Split valid and invalid source orders
    # =========================================================

    valid_orders = validated_orders.filter(
        F.col("_dq_error_reason") == ""
    )

    invalid_df = (
        validated_orders.filter(
            F.col("_dq_error_reason") != ""
        )
        .withColumn(
            "_dq_source_entity",
            F.lit("order"),
        )
    )

    # =========================================================
    # 6. Prepare valid canonical order fields
    # =========================================================

    canonical_orders = valid_orders.select(
        "order_id",
        "user_id",
        F.col("timestamp").alias(
            "order_timestamp"
        ),
        F.col("total").alias(
            "order_total"
        ),
        "status",
        "payment_method",
        F.col("kafka_timestamp").alias(
            "source_kafka_timestamp"
        ),
    )

    # =========================================================
    # 7. Resolve user surrogate key
    #
    # A structurally valid user_id that is not yet available
    # in dim_user is treated as a late-arriving dimension and
    # mapped to the Unknown User member.
    # =========================================================

    valid_df = (
        canonical_orders.alias("order")
        .join(
            dim_user_df.select(
                "user_id",
                "user_sk",
            ).alias("user"),
            F.col("order.user_id")
            == F.col("user.user_id"),
            how="left",
        )
        .select(
            F.xxhash64(
                F.col("order.order_id")
            ).alias(
                "order_sk"
            ),
            F.col(
                "order.order_id"
            ).alias(
                "order_id"
            ),
            F.coalesce(
                F.col("user.user_sk"),
                F.lit(-1).cast("bigint"),
            ).alias(
                "user_sk"
            ),
            F.date_format(
                F.col("order.order_timestamp"),
                "yyyyMMdd",
            )
            .cast("int")
            .alias(
                "order_date_sk"
            ),
            F.col(
                "order.order_timestamp"
            ).alias(
                "order_timestamp"
            ),
            F.col(
                "order.order_total"
            ).alias(
                "order_total"
            ),
            F.col(
                "order.status"
            ).alias(
                "status"
            ),
            F.col(
                "order.payment_method"
            ).alias(
                "payment_method"
            ),
            F.col(
                "order.source_kafka_timestamp"
            ).alias(
                "source_kafka_timestamp"
            ),
        )
    )

    return (
        valid_df,
        invalid_df,
    )
