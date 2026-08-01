from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_return_refund_obt(
    fact_return_refund: DataFrame,
    fact_order_item: DataFrame,
    fact_order: DataFrame,
    dim_user: DataFrame,
    dim_product: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    """Build one denormalized Gold row per return/refund."""
    return (
        fact_return_refund.alias("r")
        .join(
            fact_order_item.alias("i"),
            F.col("r.order_item_sk") == F.col("i.order_item_sk"),
            "inner",
        )
        .join(
            fact_order.alias("o"),
            F.col("r.order_sk") == F.col("o.order_sk"),
            "inner",
        )
        .join(
            dim_user.alias("u"),
            F.col("o.user_sk") == F.col("u.user_sk"),
            "inner",
        )
        .join(
            dim_product.alias("p"),
            F.col("i.product_sk") == F.col("p.product_sk"),
            "inner",
        )
        .join(
            dim_date.alias("d"),
            F.col("r.return_date_sk") == F.col("d.date_sk"),
            "inner",
        )
        .select(
            F.col("r.return_refund_sk").alias("return_refund_sk"),
            F.col("r.return_refund_id").alias("return_refund_id"),
            F.col("r.return_timestamp").alias("return_timestamp"),
            F.col("r.return_date_sk").alias("return_date_sk"),
            F.col("d.full_date").alias("return_date"),
            F.col("d.year").alias("return_year"),
            F.col("d.quarter").alias("return_quarter"),
            F.col("d.month").alias("return_month"),
            F.col("d.month_name").alias("return_month_name"),
            F.col("d.week_of_year").alias("return_week_of_year"),
            F.col("d.day").alias("return_day"),
            F.col("d.day_of_week").alias("return_day_of_week"),
            F.col("d.day_name").alias("return_day_name"),
            F.col("d.is_weekend").cast("int").alias("return_is_weekend"),
            F.col("r.refund_amount").alias("refund_amount"),
            F.col("r.return_reason").alias("return_reason"),
            F.col("o.order_sk").alias("order_sk"),
            F.col("o.order_id").alias("order_id"),
            F.col("o.order_timestamp").alias("order_timestamp"),
            F.col("o.order_total").alias("order_total"),
            F.col("o.status").alias("order_status"),
            F.col("o.payment_method").alias("payment_method"),
            F.col("i.order_item_sk").alias("order_item_sk"),
            F.col("i.order_item_id").alias("order_item_id"),
            F.col("i.quantity").alias("quantity"),
            F.col("i.unit_price").alias("unit_price"),
            F.col("i.item_total_amount").alias("item_total_amount"),
            F.col("u.user_sk").alias("user_sk"),
            F.col("u.user_id").alias("user_id"),
            F.col("u.username").alias("username"),
            F.col("u.email").alias("email"),
            F.col("u.signup_date").alias("signup_date"),
            F.col("u.device").alias("device"),
            F.col("u.loyalty_tier").alias("loyalty_tier"),
            F.col("u.location").alias("location"),
            F.col("p.product_sk").alias("product_sk"),
            F.col("p.product_id").alias("product_id"),
            F.col("p.product_name").alias("product_name"),
            F.col("p.price").alias("product_price"),
            F.current_timestamp().alias("gold_loaded_at"),
        )
    )
