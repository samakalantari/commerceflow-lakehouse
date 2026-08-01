# load_gold_behavioral_session.py
import argparse
import os

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from spark_apps.silver.config.iceberg import (
    ICEBERG_CATALOG_NAME,
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    DIM_USER,
    FACT_ORDER,
    FACT_ORDER_ITEM,
)

CATALOG = ICEBERG_CATALOG_NAME
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")
CH_DB = os.environ.get("CLICKHOUSE_DB", "gold")
CH_USER = os.environ.get("CLICKHOUSE_USER", "default")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "")
CH_WRITE_NUM_PARTITIONS = int(os.environ.get("CH_WRITE_NUM_PARTITIONS", "8"))
CH_WRITE_BATCHSIZE = int(os.environ.get("CH_WRITE_BATCHSIZE", "100000"))
SPARK_SHUFFLE_PARTITIONS = int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "32"))

JDBC_URL = f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DB}"
JDBC_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"
TARGET_TABLE = "gold_behavioral_session"


def get_args():
    p = argparse.ArgumentParser()
    p.add_argument("--start-date", required=True)
    p.add_argument("--end-date-exclusive", required=True)
    return p.parse_args()


def build_spark():
    # Reuse the same JdbcCatalog configuration used by Silver jobs,
    # so fact_order / fact_order_item / dim_user resolve to the
    # same catalog and metadata backend that produced them.
    spark = build_iceberg_spark("behavioral_gold_session_etl")
    spark.conf.set("spark.sql.shuffle.partitions", str(SPARK_SHUFFLE_PARTITIONS))
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def get_current_dim_user(spark):
    du = spark.table(DIM_USER)
    w = Window.partitionBy("user_id").orderBy(F.desc("silver_updated_at"), F.desc("silver_created_at"))
    du_current = (
        du.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .select(
              F.col("user_id"),
              F.col("location").alias("country"),
              F.col("loyalty_tier").alias("user_segment"),
          )
    )
    return du_current


def execute_clickhouse_http(sql: str) -> None:
    import urllib.request
    import urllib.parse

    params = urllib.parse.urlencode({"user": CH_USER, "password": CH_PASS})
    url = f"http://{CH_HOST}:{CH_PORT}/?{params}"
    req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    with urllib.request.urlopen(req) as resp:
        if resp.status != 200:
            raise RuntimeError(f"ClickHouse HTTP error {resp.status}: {resp.read().decode()}")


def delete_existing_range(start_date: str, end_date_exclusive: str) -> None:
    execute_clickhouse_http(
        f"ALTER TABLE {CH_DB}.{TARGET_TABLE} DELETE "
        f"WHERE session_date >= toDate('{start_date}') AND session_date < toDate('{end_date_exclusive}')"
    )


def run(spark, start_date: str, end_date_exclusive: str):
    be = spark.table(f"{CATALOG}.silver.fact_behavioral_event").filter(
        (F.col("event_timestamp") >= F.lit(start_date)) &
        (F.col("event_timestamp") < F.lit(end_date_exclusive))
    )

    w_sess = Window.partitionBy("session_id").orderBy("event_timestamp")
    w_sess_desc = Window.partitionBy("session_id").orderBy(F.desc("event_timestamp"))

    be = (
        be
        .withColumn("_rn_asc", F.row_number().over(w_sess))
        .withColumn("_rn_desc", F.row_number().over(w_sess_desc))
    )

    sess = be.groupBy("session_id").agg(
        F.first(F.when(F.col("_rn_asc") == 1, F.col("user_id"))).alias("user_id"),
        F.first(F.when(F.col("_rn_asc") == 1, F.col("device"))).alias("device"),
        F.min("event_timestamp").alias("session_start"),
        F.max("event_timestamp").alias("session_end"),
        F.first(F.when(F.col("_rn_asc") == 1, F.col("url_path"))).alias("landing_url"),
        F.first(F.when(F.col("_rn_desc") == 1, F.col("url_path"))).alias("exit_url"),
        F.first(F.when(F.col("_rn_asc") == 1, F.col("event_type"))).alias("first_event_type"),
        F.first(F.when(F.col("_rn_desc") == 1, F.col("event_type"))).alias("last_event_type"),

        F.min(F.when(F.col("event_type") == "search", F.col("event_timestamp"))).alias("first_search_timestamp"),
        F.min(F.when(F.col("event_type") == "add_to_cart", F.col("event_timestamp"))).alias("first_cart_timestamp"),
        F.min(F.when(F.col("event_type") == "checkout", F.col("event_timestamp"))).alias("first_checkout_timestamp"),
        F.min(F.when(F.col("event_type") == "payment_attempt", F.col("event_timestamp"))).alias("first_payment_timestamp"),
        F.min(F.when(F.col("order_id").isNotNull(), F.col("event_timestamp"))).alias("first_order_timestamp"),

        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("event_type") == "page_view", F.lit(1)).otherwise(0)).alias("page_view_count"),
        F.sum(F.when(F.col("event_type") == "search", F.lit(1)).otherwise(0)).alias("search_count"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.lit(1)).otherwise(0)).alias("add_to_cart_count"),
        F.sum(F.when(F.col("event_type") == "checkout", F.lit(1)).otherwise(0)).alias("checkout_count"),
        F.sum(F.when(F.col("event_type") == "payment_attempt", F.lit(1)).otherwise(0)).alias("payment_attempt_count"),
        F.sum(F.when((F.col("event_type") == "payment_attempt") & (F.col("success") == True), F.lit(1)).otherwise(0)).alias("successful_payment_count"),
        F.sum(F.when((F.col("event_type") == "payment_attempt") & (F.col("success") == False), F.lit(1)).otherwise(0)).alias("failed_payment_count"),

        F.max(F.when(F.col("event_type") == "page_view", F.lit(1)).otherwise(0)).alias("had_page_view"),
        F.max(F.when(F.col("event_type") == "search", F.lit(1)).otherwise(0)).alias("had_search"),
        F.max(F.when(F.col("event_type") == "add_to_cart", F.lit(1)).otherwise(0)).alias("had_add_to_cart"),
        F.max(F.when(F.col("event_type") == "checkout", F.lit(1)).otherwise(0)).alias("had_checkout"),
        F.max(F.when(F.col("event_type") == "payment_attempt", F.lit(1)).otherwise(0)).alias("had_payment_attempt"),
        F.max(F.when((F.col("event_type") == "payment_attempt") & (F.col("success") == True), F.lit(1)).otherwise(0)).alias("had_successful_payment"),
        F.max(F.when(F.col("order_id").isNotNull(), F.lit(1)).otherwise(0)).alias("had_behavioral_order"),

        F.countDistinct(F.when(F.col("event_type") == "page_view", F.col("product_id"))).alias("distinct_products_viewed"),
        F.countDistinct(F.when(F.col("event_type") == "add_to_cart", F.col("product_id"))).alias("distinct_products_added"),
    )

    sess = sess.withColumn(
        "session_duration_sec",
        (F.unix_timestamp("session_end") - F.unix_timestamp("session_start")).cast("int")
    ).withColumn(
        "session_date", F.to_date("session_start")
    )

    date_filter = (
        (F.col("order_timestamp") >= F.lit(start_date)) &
        (F.col("order_timestamp") < F.lit(end_date_exclusive))
    )

    be_order_map = (
        be.filter(F.col("order_id").isNotNull())
          .select("session_id", "order_id")
          .distinct()
    )

    fo = (
        spark.table(FACT_ORDER)
        .filter(date_filter)
        .select("order_id", "order_sk")
    )

    foi = (
        spark.table(FACT_ORDER_ITEM)
        .filter(date_filter)
        .select("order_id", "quantity", "item_total_amount")
    )

    # fo is expected to be much smaller than foi (one row per order
    # vs one row per order item), so broadcast it to avoid a
    # shuffle join on the high-volume fact_order_item table.
    order_agg = (
        F.broadcast(fo).join(foi, "order_id", "left")
          .groupBy("order_id")
          .agg(
              F.count("*").alias("order_item_count"),
              F.sum("quantity").alias("units_sold"),
              F.sum("item_total_amount").alias("order_revenue"),
          )
    )

    sess_order = (
        be_order_map
        .join(order_agg, "order_id", "inner")
        .groupBy("session_id")
        .agg(
            F.lit(1).cast("int").alias("had_transactional_order"),
            F.countDistinct("order_id").alias("order_count"),
            F.sum("order_item_count").alias("order_item_count"),
            F.sum("units_sold").alias("units_sold"),
            F.sum("order_revenue").alias("order_revenue"),
        )
    )

    du_current = get_current_dim_user(spark)

    result = (
        sess
        .join(sess_order, "session_id", "left")
        # dim_user is small relative to sessions; broadcast avoids a shuffle here.
        .join(F.broadcast(du_current), "user_id", "left")
    )

    result = result.withColumn(
        "had_transactional_order", F.coalesce(F.col("had_transactional_order"), F.lit(0)).cast("int")
    ).withColumn(
        "order_count", F.coalesce(F.col("order_count"), F.lit(0)).cast("int")
    ).withColumn(
        "order_item_count", F.coalesce(F.col("order_item_count"), F.lit(0)).cast("int")
    ).withColumn(
        "units_sold", F.coalesce(F.col("units_sold"), F.lit(0)).cast("int")
    ).withColumn(
        "order_revenue", F.coalesce(F.col("order_revenue"), F.lit(0)).cast("decimal(18,4)")
    ).withColumn(
        "country", F.coalesce(F.col("country"), F.lit("")).cast("string")
    ).withColumn(
        "user_segment", F.coalesce(F.col("user_segment"), F.lit("")).cast("string")
    ).withColumn(
        "acquisition_channel", F.lit("").cast("string")
    )

    result = result.withColumn(
        "order_match_status",
        F.when(
            (F.col("had_behavioral_order") == 1) & (F.col("had_transactional_order") == 1), "matched"
        ).when(
            (F.col("had_behavioral_order") == 1) & (F.col("had_transactional_order") == 0), "behavioral_only"
        ).when(
            (F.col("had_behavioral_order") == 0) & (F.col("had_transactional_order") == 1), "transactional_only"
        ).otherwise("none")
    )

    result = result.withColumn(
        "is_bounce",
        F.when(F.col("event_count") == 1, F.lit(1)).otherwise(F.lit(0)).cast("int")
    ).withColumn(
        "is_converted",
        F.when(F.col("had_successful_payment") == 1, F.lit(1)).otherwise(F.lit(0)).cast("int")
    ).withColumn(
        "is_cart_abandoned",
        F.when(
            (F.col("had_add_to_cart") == 1) & (F.col("had_successful_payment") == 0), F.lit(1)
        ).otherwise(F.lit(0)).cast("int")
    ).withColumn(
        "is_checkout_abandoned",
        F.when(
            (F.col("had_checkout") == 1) & (F.col("had_successful_payment") == 0), F.lit(1)
        ).otherwise(F.lit(0)).cast("int")
    ).withColumn(
        "etl_loaded_at", F.current_timestamp()
    )

    final = result.select(
        F.col("session_date").cast("date"),
        F.col("session_id").cast("string"),
        F.col("user_id").cast("string"),
        F.col("session_start").cast("timestamp"),
        F.col("session_end").cast("timestamp"),
        F.col("session_duration_sec").cast("int"),
        F.col("device").cast("string"),
        F.col("country").cast("string"),
        F.col("user_segment").cast("string"),
        F.col("acquisition_channel").cast("string"),
        F.col("landing_url").cast("string"),
        F.col("exit_url").cast("string"),
        F.col("first_event_type").cast("string"),
        F.col("last_event_type").cast("string"),
        F.col("first_search_timestamp").cast("timestamp"),
        F.col("first_cart_timestamp").cast("timestamp"),
        F.col("first_checkout_timestamp").cast("timestamp"),
        F.col("first_payment_timestamp").cast("timestamp"),
        F.col("first_order_timestamp").cast("timestamp"),
        F.col("event_count").cast("int"),
        F.col("page_view_count").cast("int"),
        F.col("search_count").cast("int"),
        F.col("add_to_cart_count").cast("int"),
        F.col("checkout_count").cast("int"),
        F.col("payment_attempt_count").cast("int"),
        F.col("successful_payment_count").cast("int"),
        F.col("failed_payment_count").cast("int"),
        F.col("had_page_view").cast("int"),
        F.col("had_search").cast("int"),
        F.col("had_add_to_cart").cast("int"),
        F.col("had_checkout").cast("int"),
        F.col("had_payment_attempt").cast("int"),
        F.col("had_successful_payment").cast("int"),
        F.col("had_behavioral_order").cast("int"),
        F.col("had_transactional_order").cast("int"),
        F.col("order_count").cast("int"),
        F.col("order_item_count").cast("int"),
        F.col("units_sold").cast("int"),
        F.col("order_revenue").cast("decimal(18,4)"),
        F.col("order_match_status").cast("string"),
        F.col("is_bounce").cast("int"),
        F.col("is_converted").cast("int"),
        F.col("is_cart_abandoned").cast("int"),
        F.col("is_checkout_abandoned").cast("int"),
        F.col("distinct_products_viewed").cast("int"),
        F.col("distinct_products_added").cast("int"),
        F.col("etl_loaded_at").cast("timestamp"),
    )

    final = final.repartition(CH_WRITE_NUM_PARTITIONS)

    delete_existing_range(start_date, end_date_exclusive)

    (
        final.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", TARGET_TABLE)
        .option("driver", JDBC_DRIVER)
        .option("user", CH_USER)
        .option("password", CH_PASS)
        .option("batchsize", str(CH_WRITE_BATCHSIZE))
        .option("numPartitions", str(CH_WRITE_NUM_PARTITIONS))
        .mode("append")
        .save()
    )


if __name__ == "__main__":
    args = get_args()
    spark = build_spark()
    try:
        run(spark, args.start_date, args.end_date_exclusive)
    finally:
        spark.stop()
