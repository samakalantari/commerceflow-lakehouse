from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_fact_order_source(
    orders_df: DataFrame,
    dim_user_df: DataFrame,
) -> DataFrame:
    """
    Build fact_order source.

    Grain:
        One row per order.
    """

    cleaned_orders = (
        orders_df
        .filter(
            F.col("order_id").isNotNull()
        )
        .filter(
            F.col("user_id").isNotNull()
        )
        .filter(
            F.col("timestamp").isNotNull()
        )
        .filter(
            F.col("total").isNotNull()
        )
        .select(
            F.trim(
                F.col("order_id")
            ).alias(
                "order_id"
            ),
            F.trim(
                F.col("user_id")
            ).alias(
                "user_id"
            ),
            F.col(
                "timestamp"
            ).alias(
                "order_timestamp"
            ),
            F.col(
                "total"
            ).alias(
                "order_total"
            ),
            F.trim(
                F.col("status")
            ).alias(
                "status"
            ),
            F.trim(
                F.col("payment_method")
            ).alias(
                "payment_method"
            ),
            F.col(
                "kafka_timestamp"
            ).alias(
                "source_kafka_timestamp"
            ),
        )
    )

    return (
        cleaned_orders.alias("o")
        .join(
            dim_user_df.alias("u"),
            F.col("o.user_id")
            ==
            F.col("u.user_id"),
            how="left",
        )
        .select(
            F.xxhash64(
                F.col(
                    "o.order_id"
                )
            ).alias(
                "order_sk"
            ),

            F.col(
                "o.order_id"
            ),

            F.col(
                "u.user_sk"
            ),

            F.date_format(
                F.col(
                    "o.order_timestamp"
                ),
                "yyyyMMdd",
            ).cast(
                "int"
            ).alias(
                "order_date_sk"
            ),

            F.col(
                "o.order_timestamp"
            ),

            F.col(
                "o.order_total"
            ),

            F.col(
                "o.status"
            ),

            F.col(
                "o.payment_method"
            ),

            F.col(
                "o.source_kafka_timestamp"
            ),
        )
    )
