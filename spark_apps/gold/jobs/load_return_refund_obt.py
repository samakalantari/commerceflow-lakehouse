from pyspark.sql import functions as F

from spark_apps.gold.common.clickhouse import (
    execute_clickhouse,
    read_clickhouse_table,
    write_clickhouse,
)
from spark_apps.gold.config.clickhouse import CLICKHOUSE_DATABASE
from spark_apps.gold.config.tables import (
    RETURN_REFUND_OBT,
    RETURN_REFUND_OBT_STAGING,
)
from spark_apps.gold.transforms.return_refund_obt import build_return_refund_obt
from spark_apps.silver.config.iceberg import build_iceberg_spark
from spark_apps.silver.config.tables import (
    DIM_DATE,
    DIM_PRODUCT,
    DIM_USER,
    FACT_ORDER,
    FACT_ORDER_ITEM,
    FACT_RETURN_REFUND,
)


def create_table_sql(table: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table}
    (
        return_refund_sk Int64,
        return_refund_id String,
        return_timestamp DateTime64(3),
        return_date_sk Int32,
        return_date Date,
        return_year Int32,
        return_quarter Int32,
        return_month Int32,
        return_month_name String,
        return_week_of_year Int32,
        return_day Int32,
        return_day_of_week Int32,
        return_day_name String,
        return_is_weekend Int32,
        refund_amount Decimal(10,2),
        return_reason String,
        order_sk Int64,
        order_id String,
        order_timestamp DateTime64(3),
        order_total Decimal(10,2),
        order_status Nullable(String),
        payment_method Nullable(String),
        order_item_sk Int64,
        order_item_id String,
        quantity Int32,
        unit_price Decimal(10,2),
        item_total_amount Nullable(Decimal(10,2)),
        user_sk Int64,
        user_id String,
        username String,
        email String,
        signup_date Date,
        device String,
        loyalty_tier String,
        location String,
        product_sk Int64,
        product_id String,
        product_name Nullable(String),
        product_price Nullable(Decimal(10,2)),
        gold_loaded_at DateTime64(3)
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(return_date)
    ORDER BY (return_date, return_refund_id)
    """


def main() -> None:
    spark = build_iceberg_spark("gold-load-return-refund-obt")
    source_df = None

    try:
        print("=" * 100)
        print("GOLD RETURN/REFUND OBT LOAD")
        print("=" * 100)

        execute_clickhouse(
            f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE} ENGINE = Atomic"
        )
        execute_clickhouse(create_table_sql(RETURN_REFUND_OBT))
        execute_clickhouse(create_table_sql(RETURN_REFUND_OBT_STAGING))
        execute_clickhouse(f"TRUNCATE TABLE {RETURN_REFUND_OBT_STAGING}")

        fact_return_refund = spark.table(FACT_RETURN_REFUND)
        source_df = build_return_refund_obt(
            fact_return_refund,
            spark.table(FACT_ORDER_ITEM),
            spark.table(FACT_ORDER),
            spark.table(DIM_USER),
            spark.table(DIM_PRODUCT),
            spark.table(DIM_DATE),
        ).cache()

        silver_count = fact_return_refund.count()
        source_count = source_df.count()
        distinct_count = source_df.select("return_refund_sk").distinct().count()
        duplicate_count = (
            source_df.groupBy("return_refund_sk")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        silver_refund_amount = fact_return_refund.agg(
            F.coalesce(
                F.sum("refund_amount"),
                F.lit(0).cast("decimal(10,2)"),
            ).alias("value")
        ).first()["value"]
        source_refund_amount = source_df.agg(
            F.coalesce(
                F.sum("refund_amount"),
                F.lit(0).cast("decimal(10,2)"),
            ).alias("value")
        ).first()["value"]

        if (
            source_count != silver_count
            or distinct_count != source_count
            or duplicate_count != 0
            or source_refund_amount != silver_refund_amount
        ):
            raise RuntimeError("Return/refund OBT source audit failed.")

        write_clickhouse(source_df, RETURN_REFUND_OBT_STAGING)
        staging_df = read_clickhouse_table(spark, RETURN_REFUND_OBT_STAGING)
        staging_count = staging_df.count()
        staging_refund_amount = staging_df.agg(
            F.coalesce(
                F.sum("refund_amount"),
                F.lit(0).cast("decimal(10,2)"),
            ).alias("value")
        ).first()["value"]

        if (
            staging_count != source_count
            or staging_refund_amount != source_refund_amount
        ):
            raise RuntimeError("Return/refund OBT staging audit failed.")

        execute_clickhouse(
            f"EXCHANGE TABLES {RETURN_REFUND_OBT} "
            f"AND {RETURN_REFUND_OBT_STAGING}"
        )

        target_count = read_clickhouse_table(spark, RETURN_REFUND_OBT).count()
        if target_count != source_count:
            raise RuntimeError("Published return/refund OBT row count mismatch.")

        print(f"[PASS] RETURN/REFUND OBT PUBLISHED: {target_count:,} rows")
    finally:
        if source_df is not None:
            source_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
