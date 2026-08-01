from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import date, datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


@dataclass(frozen=True)
class JobConfig:
    iceberg_catalog: str = os.getenv("ICEBERG_CATALOG_NAME", "lakehouse")

    behavioral_table: str = os.getenv(
        "BEHAVIORAL_TABLE",
        f"{os.getenv('ICEBERG_CATALOG_NAME', 'lakehouse')}.silver.fact_behavioral_event",
    )
    orders_table: str = os.getenv(
        "ORDERS_TABLE",
        f"{os.getenv('ICEBERG_CATALOG_NAME', 'lakehouse')}.silver.fact_order",
    )
    order_items_table: str = os.getenv(
        "ORDER_ITEMS_TABLE",
        f"{os.getenv('ICEBERG_CATALOG_NAME', 'lakehouse')}.silver.fact_order_item",
    )

    target_table: str = os.getenv("CH_TARGET_TABLE", "gold.gold_behavioral_daily")
    max_days_per_run: int = int(os.getenv("MAX_DAYS_PER_RUN", "35"))
    spark_shuffle_partitions: int = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))

    ch_host: str = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_http_port: str = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
    ch_user: str = os.getenv("CLICKHOUSE_USER", "default")
    ch_password: str = os.getenv("CLICKHOUSE_PASSWORD", "")
    ch_db: str = os.getenv("CLICKHOUSE_DB", "gold")
    ch_write_num_partitions: int = int(os.getenv("CH_WRITE_NUM_PARTITIONS", "8"))
    ch_write_batchsize: int = int(os.getenv("CH_WRITE_BATCHSIZE", "100000"))


def get_spark_session(cfg: JobConfig):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("load_gold_behavioral_daily")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(cfg.spark_shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def parse_ymd(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def validate_range(start_date: str, end_date: str, max_days: int) -> None:
    start = parse_ymd(start_date)
    end = parse_ymd(end_date)
    if start > end:
        raise ValueError(f"start_date > end_date: {start_date} > {end_date}")
    if (end - start).days + 1 > max_days:
        raise ValueError(f"Range too large: {(end - start).days + 1} days, max={max_days}")


def filter_by_date(df: DataFrame, start_date: str, end_date: str, col: str) -> DataFrame:
    return df.filter((F.col(col) >= F.lit(start_date)) & (F.col(col) <= F.lit(end_date)))


def load_behavioral_base(spark, cfg: JobConfig, start_date: str, end_date: str) -> DataFrame:
    df = spark.table(cfg.behavioral_table).withColumn("event_date", F.to_date("event_timestamp"))
    df = filter_by_date(df, start_date, end_date, "event_date")
    return df.withColumn(
        "device",
        F.when(F.col("device").isNull() | (F.trim(F.col("device")) == ""), F.lit("unknown")).otherwise(F.lower(F.col("device"))),
    )


def load_orders_base(spark, cfg: JobConfig, start_date: str, end_date: str) -> DataFrame:
    df = spark.table(cfg.orders_table).withColumn("order_date", F.to_date("order_timestamp"))
    df = filter_by_date(df, start_date, end_date, "order_date")
    return df.select(
        F.col("order_id").cast("string"),
        F.col("user_sk").cast("long"),
        F.col("order_date").alias("event_date"),
        F.coalesce(F.col("order_total"), F.lit(0)).cast(T.DecimalType(18, 4)).alias("order_total"),
    )


def load_order_items_base(spark, cfg: JobConfig, start_date: str, end_date: str) -> DataFrame:
    df = spark.table(cfg.order_items_table).withColumn("order_date", F.to_date("order_timestamp"))
    df = filter_by_date(df, start_date, end_date, "order_date")
    return df.select(
        F.col("order_id").cast("string"),
        F.coalesce(F.col("quantity"), F.lit(0)).cast("long").alias("quantity"),
        F.coalesce(F.col("item_total_amount"), F.lit(0)).cast(T.DecimalType(18, 4)).alias("item_total_amount"),
    )


def aggregate_behavioral(df: DataFrame) -> DataFrame:
    session_event_counts = df.groupBy("session_id").agg(F.count("*").alias("_sec"))
    df = df.join(session_event_counts, on="session_id", how="left")
    return df.groupBy("event_date", "device").agg(
        F.countDistinct("user_id").alias("unique_users"),
        F.countDistinct("session_id").alias("sessions"),
        F.count("*").alias("total_events"),
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
        F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0)).alias("searches"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_events"),
        F.sum(F.when(F.col("event_type") == "checkout_start", 1).otherwise(0)).alias("checkout_start_events"),
        F.sum(F.when(F.col("event_type") == "payment_attempt", 1).otherwise(0)).alias("payment_attempt_events"),
        F.sum(F.when(F.col("event_type") == "successful_payment", 1).otherwise(0)).alias("successful_payment_events"),
        F.sum(F.when(F.col("event_type") == "failed_payment", 1).otherwise(0)).alias("failed_payment_events"),
        F.sum(F.when(F.col("event_type") == "order_complete", 1).otherwise(0)).alias("order_complete_events"),
        F.sum(F.when(F.col("event_type") == "wishlist_add", 1).otherwise(0)).alias("wishlist_add_events"),
        F.sum(F.when(F.col("event_type") == "review", 1).otherwise(0)).alias("review_events"),
        F.countDistinct(F.when(F.col("event_type") == "page_view", F.col("user_id"))).alias("page_view_users"),
        F.countDistinct(F.when(F.col("event_type") == "search", F.col("user_id"))).alias("search_users"),
        F.countDistinct(F.when(F.col("event_type") == "add_to_cart", F.col("user_id"))).alias("add_to_cart_users"),
        F.countDistinct(F.when(F.col("event_type") == "checkout_start", F.col("user_id"))).alias("checkout_users"),
        F.countDistinct(F.when(F.col("event_type") == "payment_attempt", F.col("user_id"))).alias("payment_attempt_users"),
        F.countDistinct(F.when(F.col("event_type") == "successful_payment", F.col("user_id"))).alias("successful_payment_users"),
        F.countDistinct(F.when(F.col("event_type") == "order_complete", F.col("user_id"))).alias("order_users"),
        F.countDistinct(F.when(F.col("event_type") == "page_view", F.col("session_id"))).alias("page_view_sessions"),
        F.countDistinct(F.when(F.col("event_type") == "search", F.col("session_id"))).alias("search_sessions"),
        F.countDistinct(F.when(F.col("event_type") == "add_to_cart", F.col("session_id"))).alias("add_to_cart_sessions"),
        F.countDistinct(F.when(F.col("event_type") == "checkout_start", F.col("session_id"))).alias("checkout_sessions"),
        F.countDistinct(F.when(F.col("event_type") == "payment_attempt", F.col("session_id"))).alias("payment_attempt_sessions"),
        F.countDistinct(F.when(F.col("event_type") == "successful_payment", F.col("session_id"))).alias("successful_payment_sessions"),
        F.countDistinct(F.when(F.col("event_type") == "order_complete", F.col("session_id"))).alias("order_sessions"),
        F.sum(F.when((F.col("event_type") == "search") & (F.coalesce(F.col("results_count"), F.lit(0)) == 0), 1).otherwise(0)).alias("zero_result_searches"),
        F.sum(F.when((F.col("event_type") == "search") & F.col("clicked_position").isNotNull(), 1).otherwise(0)).alias("clicked_searches"),
        F.sum(F.when(F.col("http_status").between(400, 499), 1).otherwise(0)).alias("http_4xx_events"),
        F.sum(F.when(F.col("http_status").between(500, 599), 1).otherwise(0)).alias("http_5xx_events"),
        F.sum(F.coalesce(F.col("duration_sec"), F.lit(0))).cast("long").alias("total_session_duration_sec"),
        F.countDistinct(
            F.when((F.col("_sec") <= 1) & F.col("session_id").isNotNull(), F.col("session_id"))
        ).alias("bounced_sessions"),
    )


def compute_new_users(df: DataFrame) -> DataFrame:
    first_seen = df.groupBy("user_id").agg(F.min("event_date").alias("first_seen_date"))
    return (
        df.select("event_date", "device", "user_id")
        .dropDuplicates()
        .join(first_seen, on="user_id", how="left")
        .withColumn("is_new", F.when(F.col("event_date") == F.col("first_seen_date"), 1).otherwise(0))
        .groupBy("event_date", "device")
        .agg(F.sum("is_new").cast("long").alias("new_users"))
    )


def compute_returning_users(df: DataFrame, new_users: DataFrame) -> DataFrame:
    uniq = df.groupBy("event_date", "device").agg(F.countDistinct("user_id").alias("unique_users"))
    return (
        uniq.join(new_users, on=["event_date", "device"], how="left")
        .fillna({"new_users": 0})
        .withColumn("returning_users", F.greatest(F.col("unique_users") - F.col("new_users"), F.lit(0)).cast("long"))
        .select("event_date", "device", "returning_users")
    )


def compute_commercial_metrics(orders: DataFrame, items: DataFrame) -> DataFrame:
    item_agg = items.groupBy("order_id").agg(
        F.sum("quantity").cast("long").alias("units_sold"),
        F.sum("item_total_amount").cast(T.DecimalType(18, 4)).alias("items_revenue"),
    )
    o = orders.join(item_agg, on="order_id", how="left").fillna({"units_sold": 0, "items_revenue": 0})
    return o.groupBy("event_date").agg(
        F.countDistinct("order_id").alias("order_count"),
        F.countDistinct("user_sk").alias("paying_users"),
        F.sum("units_sold").cast("long").alias("units_sold"),
        F.sum("order_total").cast(T.DecimalType(18, 4)).alias("gross_revenue"),
        F.lit(0).cast(T.DecimalType(18, 4)).alias("discount_amount"),
        F.lit(0).cast(T.DecimalType(18, 4)).alias("refund_amount"),
        F.sum("order_total").cast(T.DecimalType(18, 4)).alias("net_revenue"),
    )


def build_gold_daily(spark, cfg: JobConfig, start_date: str, end_date: str) -> DataFrame:
    behavioral = load_behavioral_base(spark, cfg, start_date, end_date)
    behavioral_all = behavioral.withColumn("device", F.lit("all"))
    behavioral_combined = behavioral.unionByName(behavioral_all)

    behavioral_aggs = aggregate_behavioral(behavioral).unionByName(
        aggregate_behavioral(behavioral_all), allowMissingColumns=True
    )

    new_users = compute_new_users(behavioral).unionByName(compute_new_users(behavioral_all))
    returning = compute_returning_users(behavioral_combined, new_users)

    behavioral_final = (
        behavioral_aggs
        .join(new_users, on=["event_date", "device"], how="left")
        .join(returning, on=["event_date", "device"], how="left")
        .fillna({"new_users": 0, "returning_users": 0})
    )

    orders = load_orders_base(spark, cfg, start_date, end_date)
    items = load_order_items_base(spark, cfg, start_date, end_date)
    commercial = compute_commercial_metrics(orders, items).withColumn("device", F.lit("all"))

    final_df = (
        behavioral_final
        .join(commercial, on=["event_date", "device"], how="left")
        .fillna({"order_count": 0, "paying_users": 0, "units_sold": 0,
                 "gross_revenue": 0, "net_revenue": 0, "discount_amount": 0, "refund_amount": 0})
        .withColumn("etl_loaded_at", F.current_timestamp())
    )

    return final_df.select(
        "event_date", "device",
        "unique_users", "new_users", "returning_users",
        "sessions", "total_events",
        "page_views", "searches", "add_to_cart_events", "checkout_start_events",
        "payment_attempt_events", "successful_payment_events", "failed_payment_events",
        "order_complete_events", "wishlist_add_events", "review_events",
        "page_view_users", "search_users", "add_to_cart_users", "checkout_users",
        "payment_attempt_users", "successful_payment_users", "order_users",
        "page_view_sessions", "search_sessions", "add_to_cart_sessions", "checkout_sessions",
        "payment_attempt_sessions", "successful_payment_sessions", "order_sessions",
        "order_count", "paying_users", "units_sold",
        "gross_revenue", "discount_amount", "refund_amount", "net_revenue",
        "zero_result_searches", "clicked_searches", "http_4xx_events", "http_5xx_events",
        "total_session_duration_sec", "bounced_sessions",
        "etl_loaded_at",
    )


def build_jdbc_url(cfg: JobConfig) -> str:
    return f"jdbc:clickhouse://{cfg.ch_host}:{cfg.ch_http_port}/{cfg.ch_db}"


def execute_clickhouse_http(cfg: JobConfig, sql: str) -> None:
    import urllib.request
    import urllib.parse

    params = urllib.parse.urlencode({"user": cfg.ch_user, "password": cfg.ch_password})
    url = f"http://{cfg.ch_host}:{cfg.ch_http_port}/?{params}"
    req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    with urllib.request.urlopen(req) as resp:
        if resp.status != 200:
            raise RuntimeError(f"ClickHouse HTTP error {resp.status}: {resp.read().decode()}")


def delete_existing_range(cfg: JobConfig, start_date: str, end_date: str) -> None:
    execute_clickhouse_http(
        cfg,
        f"ALTER TABLE {cfg.target_table} DELETE "
        f"WHERE event_date >= toDate('{start_date}') AND event_date <= toDate('{end_date}')",
    )


def write_to_clickhouse(df: DataFrame, cfg: JobConfig) -> None:
    (
        df.write
        .format("jdbc")
        .option("url", build_jdbc_url(cfg))
        .option("dbtable", cfg.target_table)
        .option("user", cfg.ch_user)
        .option("password", cfg.ch_password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("batchsize", str(cfg.ch_write_batchsize))
        .option("numPartitions", str(cfg.ch_write_num_partitions))
        .mode("append")
        .save()
    )


def run(start_date: str, end_date: str) -> None:
    cfg = JobConfig()
    validate_range(start_date, end_date, cfg.max_days_per_run)
    spark = get_spark_session(cfg)
    try:
        df = build_gold_daily(spark, cfg, start_date, end_date)
        df = df.repartition(cfg.ch_write_num_partitions)
        delete_existing_range(cfg, start_date, end_date)
        write_to_clickhouse(df, cfg)
        print(f"Load completed for range {start_date}..{end_date}")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()
    run(args.start_date, args.end_date)
