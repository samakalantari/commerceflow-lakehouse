from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_transactional_obt(
    fact_order_item: DataFrame,
    fact_order: DataFrame,
    dim_user: DataFrame,
    dim_product: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:

    # ========================================================
    # Join Silver Star Schema
    # ========================================================

    base = (
        fact_order_item.alias("i")
        .join(
            fact_order.alias("o"),
            F.col("i.order_sk") == F.col("o.order_sk"),
            how="inner",
        )
        .join(
            dim_user.alias("u"),
            F.col("o.user_sk") == F.col("u.user_sk"),
            how="inner",
        )
        .join(
            dim_product.alias("p"),
            F.col("i.product_sk") == F.col("p.product_sk"),
            how="inner",
        )
        .join(
            dim_date.alias("d"),
            F.col("i.order_date_sk") == F.col("d.date_sk"),
            how="inner",
        )
        .select(
            # ------------------------------------------------
            # Order Item Grain
            # ------------------------------------------------
            F.col("i.order_item_sk").alias("order_item_sk"),
            F.col("i.order_item_id").alias("order_item_id"),
            # ------------------------------------------------
            # Order
            # ------------------------------------------------
            F.col("o.order_sk").alias("order_sk"),
            F.col("o.order_id").alias("order_id"),
            F.col("o.order_timestamp").alias("order_timestamp"),
            F.col("o.order_date_sk").alias("order_date_sk"),
            # ------------------------------------------------
            # Date
            # ------------------------------------------------
            F.col("d.full_date").alias("full_date"),
            F.col("d.year").alias("year"),
            F.col("d.quarter").alias("quarter"),
            F.col("d.month").alias("month"),
            F.col("d.month_name").alias("month_name"),
            F.col("d.week_of_year").alias("week_of_year"),
            F.col("d.day").alias("day"),
            F.col("d.day_of_week").alias("day_of_week"),
            F.col("d.day_name").alias("day_name"),
            F.col("d.is_weekend").cast("int").alias("is_weekend"),
            # ------------------------------------------------
            # Order Measures / Attributes
            # ------------------------------------------------
            F.col("o.order_total").alias("order_total"),
            F.col("o.status").alias("status"),
            F.col("o.payment_method").alias("payment_method"),
            # ------------------------------------------------
            # User
            # ------------------------------------------------
            F.col("u.user_sk").alias("user_sk"),
            F.col("u.user_id").alias("user_id"),
            F.col("u.username").alias("username"),
            F.col("u.email").alias("email"),
            F.col("u.signup_date").alias("signup_date"),
            F.col("u.device").alias("device"),
            F.col("u.loyalty_tier").alias("loyalty_tier"),
            F.col("u.location").alias("location"),
            # ------------------------------------------------
            # Product
            # ------------------------------------------------
            F.col("p.product_sk").alias("product_sk"),
            F.col("p.product_id").alias("product_id"),
            F.col("p.product_name").alias("product_name"),
            F.col("p.price").alias("product_price"),
            # ------------------------------------------------
            # Item Measures
            # ------------------------------------------------
            F.col("i.quantity").alias("quantity"),
            F.col("i.unit_price").alias("unit_price"),
            F.col("i.item_total_amount").alias("item_total_amount"),
            F.col("i.product_resolution").alias("product_resolution"),
        )
    )

    # ========================================================
    # Prevent order-level double counting
    # ========================================================

    order_window = Window.partitionBy("order_sk").orderBy(F.col("order_item_sk").asc())

    order_count_window = Window.partitionBy("order_sk")

    result = (
        base.withColumn(
            "_order_row_number",
            F.row_number().over(order_window),
        )
        .withColumn(
            "order_count_flag",
            F.when(
                F.col("_order_row_number") == 1,
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "order_total_once",
            F.when(
                F.col("_order_row_number") == 1,
                F.col("order_total"),
            ).otherwise(F.lit(0).cast("decimal(10,2)")),
        )
        .withColumn(
            "item_count_in_order",
            F.count(F.lit(1)).over(order_count_window),
        )
        .drop("_order_row_number")
        .withColumn(
            "gold_loaded_at",
            F.current_timestamp(),
        )
    )

    # ========================================================
    # Exact ClickHouse column order
    # ========================================================

    return result.select(
        "order_item_sk",
        "order_item_id",
        "order_sk",
        "order_id",
        "order_timestamp",
        "order_date_sk",
        "full_date",
        "year",
        "quarter",
        "month",
        "month_name",
        "week_of_year",
        "day",
        "day_of_week",
        "day_name",
        "is_weekend",
        "order_count_flag",
        "item_count_in_order",
        "order_total",
        "order_total_once",
        "status",
        "payment_method",
        "user_sk",
        "user_id",
        "username",
        "email",
        "signup_date",
        "device",
        "loyalty_tier",
        "location",
        "product_sk",
        "product_id",
        "product_name",
        "product_price",
        "quantity",
        "unit_price",
        "item_total_amount",
        "product_resolution",
        "gold_loaded_at",
    )
