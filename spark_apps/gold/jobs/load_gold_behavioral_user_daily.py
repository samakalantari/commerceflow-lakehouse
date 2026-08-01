import argparse
import os

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_apps.silver.config.iceberg import (
    ICEBERG_CATALOG_NAME,
    build_iceberg_spark,
)

SILVER_TABLE = f"{ICEBERG_CATALOG_NAME}.silver.fact_behavioral_event"

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_HTTP_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CLICKHOUSE_URL = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/gold"
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "gold")
CLICKHOUSE_TABLE = "gold_behavioral_user_daily"


def get_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--activity-date", required=True)  # e.g. 2026-07-12
    return parser.parse_args()


def execute_clickhouse_http(sql: str) -> None:
    import urllib.request
    import urllib.parse

    params = urllib.parse.urlencode({"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD})
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/?{params}"
    req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    with urllib.request.urlopen(req) as resp:
        if resp.status != 200:
            raise RuntimeError(f"ClickHouse HTTP error {resp.status}: {resp.read().decode()}")


def delete_existing_range(activity_date: str) -> None:
    execute_clickhouse_http(
        f"ALTER TABLE {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} DELETE "
        f"WHERE activity_date = toDate('{activity_date}')"
    )


def get_first_seen_df(spark):
    """
    Computed fresh from full Silver history each run.
    No extra Iceberg table maintained.
    """
    return (
        spark.table(SILVER_TABLE)
        .filter(F.col("user_id").isNotNull())
        .groupBy("user_id")
        .agg(F.min(F.to_date(F.col("event_timestamp"))).alias("first_seen_date"))
    )


def get_lifetime_stats_df(spark, activity_date: str):
    """
    Lifetime = everything up to and including activity_date.
    Computed fresh each run. Behavioral proxy only
    (order_complete events) until fact_orders exists.
    """
    return (
        spark.table(SILVER_TABLE)
        .filter(F.to_date(F.col("event_timestamp")) <= F.lit(activity_date))
        .filter((F.col("event_type") == "order_complete") & F.col("user_id").isNotNull())
        .groupBy("user_id")
        .agg(
            F.countDistinct("order_id").alias("lifetime_orders"),
            F.max(F.to_date(F.col("event_timestamp"))).alias("last_order_date"),
        )
    )


def build_user_daily(day_df, first_seen_df, lifetime_df, activity_date: str):
    device_counts = day_df.groupBy("user_id", "device").agg(F.count("*").alias("_n"))
    device_window = Window.partitionBy("user_id").orderBy(F.desc("_n"))
    primary_device_df = (
        device_counts
        .withColumn("_rank", F.row_number().over(device_window))
        .filter(F.col("_rank") == 1)
        .select("user_id", F.col("device").alias("primary_device"))
    )

    per_user = (
        day_df.groupBy("user_id")
        .agg(
            F.countDistinct("session_id").alias("session_count"),
            F.count("*").alias("event_count"),
            F.sum(F.coalesce(F.col("duration_sec"), F.lit(0))).alias("active_duration_sec"),
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
            F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0)).alias("search_count"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
            F.sum(F.when(F.col("event_type") == "checkout_start", 1).otherwise(0)).alias("checkout_count"),
            F.sum(F.when(F.col("event_type") == "payment_attempt", 1).otherwise(0)).alias("payment_attempt_count"),
            F.countDistinct(F.when(F.col("event_type") == "order_complete", F.col("order_id"))).alias("daily_orders"),
            F.sum(F.when(F.col("event_type") == "wishlist_add", 1).otherwise(0)).alias("wishlist_count"),
            F.sum(F.when(F.col("event_type") == "review", 1).otherwise(0)).alias("review_count"),
        )
    )

    result = (
        per_user
        .join(first_seen_df, on="user_id", how="left")
        .join(primary_device_df, on="user_id", how="left")
        .join(lifetime_df, on="user_id", how="left")
        .withColumn("activity_date", F.lit(activity_date).cast("date"))
        .withColumn("cohort_week", F.date_trunc("week", F.col("first_seen_date")).cast("date"))
        .withColumn("cohort_month", F.date_trunc("month", F.col("first_seen_date")).cast("date"))
        .withColumn("days_since_first_seen", F.datediff(F.col("activity_date"), F.col("first_seen_date")))
        .withColumn("is_new_user", (F.col("first_seen_date") == F.col("activity_date")).cast("int"))
        .withColumn("is_returning_user", (F.col("first_seen_date") != F.col("activity_date")).cast("int"))
        .withColumn("order_count", F.col("daily_orders"))
        .withColumn("was_active", F.lit(1))
        .withColumn("was_searcher", (F.col("search_count") > 0).cast("int"))
        .withColumn("was_cart_user", (F.col("add_to_cart_count") > 0).cast("int"))
        .withColumn("was_checkout_user", (F.col("checkout_count") > 0).cast("int"))
        .withColumn("was_buyer", (F.col("daily_orders") > 0).cast("int"))
        .withColumn("was_reviewer", (F.col("review_count") > 0).cast("int"))
        .withColumn("daily_units", F.lit(0))
        .withColumn("daily_revenue", F.lit(0.0))
        .withColumn("lifetime_orders", F.coalesce(F.col("lifetime_orders"), F.lit(0)))
        .withColumn("lifetime_revenue", F.lit(0.0))
        .withColumn(
            "days_since_last_order",
            F.when(F.col("last_order_date").isNotNull(), F.datediff(F.col("activity_date"), F.col("last_order_date"))),
        )
        .withColumn("country", F.lit("unknown"))
        .withColumn("user_segment", F.lit("unknown"))
        .withColumn("acquisition_channel", F.lit("unknown"))
    )

    return result


def main():
    args = get_arguments()
    activity_date = args.activity_date

    spark = build_iceberg_spark(app_name="load_gold_behavioral_user_daily")

    try:
        day_df = (
            spark.table(SILVER_TABLE)
            .filter(F.to_date(F.col("event_timestamp")) == F.lit(activity_date))
        ).cache()

        first_seen_df = get_first_seen_df(spark)
        lifetime_df = get_lifetime_stats_df(spark, activity_date)

        result_df = build_user_daily(day_df, first_seen_df, lifetime_df, activity_date)

        # Idempotency fix: wipe this day's existing rows before inserting
        # the fresh recompute, so reruns/backfills never duplicate.
        delete_existing_range(activity_date)

        (
            result_df.write
            .format("jdbc")
            .option("url", CLICKHOUSE_URL)
            .option("dbtable", CLICKHOUSE_TABLE)
            .option("user", CLICKHOUSE_USER)
            .option("password", CLICKHOUSE_PASSWORD)
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
            .mode("append")
            .save()
        )

        print(f"[PASS] gold_behavioral_user_daily loaded for {activity_date}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()