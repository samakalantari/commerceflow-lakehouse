from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_fact_order_source(
    orders_df: DataFrame,
    dim_user_df: DataFrame,
) -> DataFrame:
    """
    Build canonical fact_order source.

    Grain:
        One row per order_id.

    Rules:
        - Remove invalid source rows.
        - Normalize business keys.
        - Keep the latest Kafka version of each order.
        - Resolve user_sk from dim_user.
        - Use Unknown User (-1) when no valid user is found.
    """

    # ---------------------------------------------------------
    # 1. Basic cleansing and normalization
    # ---------------------------------------------------------

    normalized_orders = (
        orders_df.filter(F.col("order_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("timestamp").isNotNull())
        .filter(F.col("total").isNotNull())
        .withColumn(
            "order_id",
            F.trim(F.col("order_id")),
        )
        .withColumn(
            "user_id",
            F.trim(F.col("user_id")),
        )
        .filter(F.length(F.col("order_id")) > 0)
        .filter(F.length(F.col("user_id")) > 0)
    )

    # ---------------------------------------------------------
    # 2. Deduplicate orders
    #
    # Bronze may contain multiple Kafka versions of the same
    # order. Keep only the latest version per order_id.
    # ---------------------------------------------------------

    latest_order_window = Window.partitionBy("order_id").orderBy(
        F.col("kafka_timestamp").desc(),
        F.col("kafka_partition").desc(),
        F.col("kafka_offset").desc(),
    )

    cleaned_orders = (
        normalized_orders.withColumn(
            "_row_number",
            F.row_number().over(latest_order_window),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
        .select(
            "order_id",
            "user_id",
            F.col("timestamp").alias("order_timestamp"),
            F.col("total").alias("order_total"),
            F.trim(F.col("status")).alias("status"),
            F.trim(F.col("payment_method")).alias("payment_method"),
            F.col("kafka_timestamp").alias("source_kafka_timestamp"),
        )
    )

    # ---------------------------------------------------------
    # 3. Resolve user surrogate key
    #
    # If no valid user exists in DIM_USER, map the order to
    # the Unknown User member with user_sk = -1.
    # ---------------------------------------------------------

    result = (
        cleaned_orders.alias("o")
        .join(
            dim_user_df.alias("u"),
            F.col("o.user_id") == F.col("u.user_id"),
            how="left",
        )
        .select(
            F.xxhash64(F.col("o.order_id")).alias("order_sk"),
            F.col("o.order_id").alias("order_id"),
            F.coalesce(
                F.col("u.user_sk"),
                F.lit(-1).cast("bigint"),
            ).alias("user_sk"),
            F.date_format(
                F.col("o.order_timestamp"),
                "yyyyMMdd",
            )
            .cast("int")
            .alias("order_date_sk"),
            F.col("o.order_timestamp").alias("order_timestamp"),
            F.col("o.order_total").alias("order_total"),
            F.col("o.status").alias("status"),
            F.col("o.payment_method").alias("payment_method"),
            F.col("o.source_kafka_timestamp").alias("source_kafka_timestamp"),
        )
    )

    return result
