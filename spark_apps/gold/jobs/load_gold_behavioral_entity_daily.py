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
CLICKHOUSE_TABLE = "gold_behavioral_entity_daily"


def get_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--activity-date",
        required=True,
    )

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
        f"WHERE event_date = toDate('{activity_date}')"
    )


def build_product_entity(day_df, sessions_with_order):

    df = day_df.filter(
        F.col("product_id").isNotNull()
    )

    return (
        df.groupBy(
            F.col("product_id").alias("entity_key"),
            "device"
        )
        .agg(
            F.count("*").alias("event_count"),

            F.countDistinct("user_id")
            .alias("unique_users"),

            F.countDistinct("session_id")
            .alias("unique_sessions"),

            F.countDistinct(
                F.when(
                    F.col("session_id").isin(
                        sessions_with_order
                    ),
                    F.col("user_id")
                )
            )
            .alias("converting_users"),

            F.countDistinct(
                F.when(
                    F.col("session_id").isin(
                        sessions_with_order
                    ),
                    F.col("session_id")
                )
            )
            .alias("converting_sessions"),

            F.sum(
                F.when(
                    F.col("event_type") == "page_view",
                    1
                )
                .otherwise(0)
            )
            .alias("product_views"),

            F.sum(
                F.when(
                    F.col("event_type") == "add_to_cart",
                    1
                )
                .otherwise(0)
            )
            .alias("add_to_cart_count"),

            F.countDistinct(
                F.when(
                    F.col("event_type") == "add_to_cart",
                    F.col("user_id")
                )
            )
            .alias("add_to_cart_users"),

            F.sum(
                F.when(
                    F.col("event_type") == "wishlist_add",
                    1
                )
                .otherwise(0)
            )
            .alias("wishlist_add_count"),

            F.sum(
                F.when(
                    F.col("event_type") == "order_complete",
                    1
                )
                .otherwise(0)
            )
            .alias("purchase_count"),

            F.sum(
                F.when(
                    F.col("event_type") == "add_to_cart",
                    F.coalesce(
                        F.col("quantity"),
                        F.lit(0)
                    )
                )
                .otherwise(0)
            )
            .alias("units_sold"),

            F.sum(
                F.when(
                    F.col("event_type") == "review",
                    1
                )
                .otherwise(0)
            )
            .alias("review_count"),

            F.sum(
                F.when(
                    F.col("event_type") == "review",
                    F.coalesce(
                        F.col("rating"),
                        F.lit(0)
                    )
                )
                .otherwise(0)
            )
            .alias("rating_sum"),
        )
        .withColumn(
            "entity_type",
            F.lit("product")
        )
    )



def build_search_entity(
    day_df,
    sessions_with_cart,
    sessions_with_order
):

    df = (
        day_df
        .filter(
            F.col("event_type") == "search"
        )
        .withColumn(
            "entity_key",
            F.trim(
                F.lower(
                    F.col("search_query")
                )
            )
        )
    )

    return (
        df.groupBy(
            "entity_key",
            "device"
        )
        .agg(

            F.count("*")
            .alias("event_count"),

            F.countDistinct("user_id")
            .alias("unique_users"),

            F.countDistinct("session_id")
            .alias("unique_sessions"),

            F.countDistinct(
                F.when(
                    F.col("session_id").isin(
                        sessions_with_order
                    ),
                    F.col("user_id")
                )
            )
            .alias("converting_users"),

            F.countDistinct(
                F.when(
                    F.col("session_id").isin(
                        sessions_with_order
                    ),
                    F.col("session_id")
                )
            )
            .alias("converting_sessions"),

            F.count("*")
            .alias("search_count"),

            F.sum(
                F.when(
                    F.col("results_count") == 0,
                    1
                )
                .otherwise(0)
            )
            .alias("zero_result_count"),

            F.sum(
                F.when(
                    F.col("clicked_position").isNotNull(),
                    1
                )
                .otherwise(0)
            )
            .alias("clicked_search_count"),

            F.sum(
                F.coalesce(
                    F.col("results_count"),
                    F.lit(0)
                )
            )
            .alias("results_count_sum"),

            F.sum(
                F.coalesce(
                    F.col("clicked_position"),
                    F.lit(0)
                )
            )
            .alias("clicked_position_sum"),

            F.countDistinct(
                F.when(
                    F.col("session_id").isin(
                        sessions_with_cart
                    ),
                    F.col("session_id")
                )
            )
            .alias("search_to_cart_sessions"),

            F.countDistinct(
                F.when(
                    F.col("session_id").isin(
                        sessions_with_order
                    ),
                    F.col("session_id")
                )
            )
            .alias("search_to_order_sessions"),
        )
        .withColumn(
            "entity_type",
            F.lit("search_query")
        )
    )



def build_page_entity(day_df, sessions_with_order):

    session_window_asc = (
        Window
        .partitionBy("session_id")
        .orderBy("event_timestamp")
    )

    session_window_desc = (
        Window
        .partitionBy("session_id")
        .orderBy(
            F.desc("event_timestamp")
        )
    )

    session_total_events = (
        Window
        .partitionBy("session_id")
    )


    flagged = (
        day_df

        .withColumn(
            "_row_asc",
            F.row_number()
            .over(session_window_asc)
        )

        .withColumn(
            "_row_desc",
            F.row_number()
            .over(session_window_desc)
        )

        .withColumn(
            "_session_event_count",
            F.count("*")
            .over(session_total_events)
        )
    )


    df = (
        flagged
        .filter(
            F.col("event_type") == "page_view"
        )
        .withColumn(
            "entity_key",
            F.col("url_path")
        )
    )


    return (
        df.groupBy(
            "entity_key",
            "device"
        )
        .agg(

            F.count("*")
            .alias("event_count"),

            F.countDistinct("user_id")
            .alias("unique_users"),

            F.countDistinct("session_id")
            .alias("unique_sessions"),

            F.countDistinct(
                F.when(
                    F.col("session_id").isin(
                        sessions_with_order
                    ),
                    F.col("user_id")
                )
            )
            .alias("converting_users"),

            F.countDistinct(
                F.when(
                    F.col("session_id").isin(
                        sessions_with_order
                    ),
                    F.col("session_id")
                )
            )
            .alias("converting_sessions"),

            F.count("*")
            .alias("page_views"),

            F.sum(
                F.coalesce(
                    F.col("duration_sec"),
                    F.lit(0)
                )
            )
            .alias("duration_sum_sec"),

            F.countDistinct(
                F.when(
                    F.col("_row_asc") == 1,
                    F.col("session_id")
                )
            )
            .alias("entrance_sessions"),

            F.countDistinct(
                F.when(
                    F.col("_row_desc") == 1,
                    F.col("session_id")
                )
            )
            .alias("exit_sessions"),

            F.countDistinct(
                F.when(
                    F.col("_session_event_count") == 1,
                    F.col("session_id")
                )
            )
            .alias("bounce_sessions"),

            F.countDistinct(
                F.when(
                    F.col("session_id").isin(
                        sessions_with_order
                    ),
                    F.col("session_id")
                )
            )
            .alias("page_conversion_sessions"),

            F.sum(
                F.when(
                    (F.col("http_status") >= 400)
                    &
                    (F.col("http_status") < 500),
                    1
                )
                .otherwise(0)
            )
            .alias("http_4xx_count"),

            F.sum(
                F.when(
                    F.col("http_status") >= 500,
                    1
                )
                .otherwise(0)
            )
            .alias("http_5xx_count"),
        )

        .withColumn(
            "entity_type",
            F.lit("page")
        )
    )



def main():

    args = get_arguments()

    activity_date = args.activity_date


    spark = build_iceberg_spark(
        app_name="load_gold_behavioral_entity_daily"
    )


    try:

        # Idempotency fix: filter by whole calendar day instead of a
        # [start_ts, end_ts) window, matching daily/user_daily/session.
        day_df = (
            spark.table(SILVER_TABLE)

            .filter(
                F.to_date(F.col("event_timestamp")) == F.lit(activity_date)
            )

        ).cache()



        sessions_with_cart = [
            r.session_id
            for r in
            day_df
            .filter(
                F.col("event_type")
                ==
                "add_to_cart"
            )
            .select("session_id")
            .distinct()
            .collect()
        ]



        sessions_with_order = [
            r.session_id
            for r in
            day_df
            .filter(
                F.col("event_type")
                ==
                "order_complete"
            )
            .select("session_id")
            .distinct()
            .collect()
        ]



        product_df = build_product_entity(
            day_df,
            sessions_with_order
        )

        search_df = build_search_entity(
            day_df,
            sessions_with_cart,
            sessions_with_order
        )

        page_df = build_page_entity(
            day_df,
            sessions_with_order
        )



        all_cols = (
            set(product_df.columns)
            |
            set(search_df.columns)
            |
            set(page_df.columns)
        )


        def align(df):

            for c in all_cols:

                if c not in df.columns:
                    df = df.withColumn(
                        c,
                        F.lit(0)
                    )

            return df.select(
                sorted(all_cols)
            )


        result_df = (
            align(product_df)

            .unionByName(
                align(search_df)
            )

            .unionByName(
                align(page_df)
            )

            .withColumn(
                "event_date",
                F.to_date(
                    F.lit(activity_date)
                )
            )

            .withColumn(
                "order_count",
                F.lit(0)
            )

            .withColumn(
                "revenue",
                F.lit(0.0)
            )

            .withColumn(
                "search_attributed_revenue",
                F.lit(0.0)
            )
        )


        result_df = result_df.localCheckpoint(
            eager=True
        )


        # Idempotency fix: wipe this day's existing rows before inserting
        # the fresh recompute, so reruns/backfills never duplicate.
        delete_existing_range(activity_date)


        (
            result_df.write

            .format("jdbc")

            .option(
                "url",
                CLICKHOUSE_URL
            )

            .option(
                "dbtable",
                CLICKHOUSE_TABLE
            )

            .option(
                "user",
                CLICKHOUSE_USER
            )

            .option(
                "password",
                CLICKHOUSE_PASSWORD
            )

            .option(
                "driver",
                "com.clickhouse.jdbc.ClickHouseDriver"
            )

            .mode("append")

            .save()
        )


        print(
            f"[PASS] Loaded gold_behavioral_entity_daily for {activity_date}"
        )


    finally:

        spark.stop()



if __name__ == "__main__":
    main()