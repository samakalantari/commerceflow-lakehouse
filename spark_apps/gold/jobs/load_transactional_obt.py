from pyspark.sql import functions as F

from spark_apps.gold.common.clickhouse import (
    execute_clickhouse,
    read_clickhouse_table,
    write_clickhouse,
)
from spark_apps.gold.config.clickhouse import (
    CLICKHOUSE_DATABASE,
)
from spark_apps.gold.config.tables import (
    TRANSACTIONAL_OBT,
    TRANSACTIONAL_OBT_STAGING,
)
from spark_apps.gold.transforms.transactional_obt import (
    build_transactional_obt,
)
from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    DIM_DATE,
    DIM_PRODUCT,
    DIM_USER,
    FACT_ORDER,
    FACT_ORDER_ITEM,
)


def create_table_sql(
    table: str,
) -> str:

    return f"""
    CREATE TABLE IF NOT EXISTS
    {table}
    (
        order_item_sk Int64,
        order_item_id String,

        order_sk Int64,
        order_id String,
        order_timestamp DateTime64(3),
        order_date_sk Int32,

        full_date Date,
        year Int32,
        quarter Int32,
        month Int32,
        month_name String,
        week_of_year Int32,
        day Int32,
        day_of_week Int32,
        day_name String,
        is_weekend Int32,

        order_count_flag Int32,
        item_count_in_order Int64,

        order_total Decimal(10,2),
        order_total_once Decimal(10,2),

        status Nullable(String),
        payment_method Nullable(String),

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

        quantity Int32,
        unit_price Decimal(10,2),
        item_total_amount Nullable(Decimal(10,2)),
        product_resolution String,

        gold_loaded_at DateTime64(3)
    )
    ENGINE = MergeTree

    PARTITION BY
        toYYYYMM(full_date)

    ORDER BY
    (
        full_date,
        product_id,
        user_id,
        order_id,
        order_item_id
    )
    """


def bootstrap_gold() -> None:

    execute_clickhouse(
        f"""
        CREATE DATABASE IF NOT EXISTS
        {CLICKHOUSE_DATABASE}
        ENGINE = Atomic
        """
    )

    engine = execute_clickhouse(
        f"""
            SELECT engine
            FROM system.databases
            WHERE name =
                '{CLICKHOUSE_DATABASE}'
            FORMAT TSVRaw
            """
    ).strip()

    if engine != "Atomic":
        raise RuntimeError(f"Gold database engine is '{engine}', expected 'Atomic'.")

    execute_clickhouse(create_table_sql(TRANSACTIONAL_OBT))

    execute_clickhouse(create_table_sql(TRANSACTIONAL_OBT_STAGING))


def main() -> None:

    spark = build_iceberg_spark("gold-load-transactional-obt")

    try:
        print("=" * 100)
        print("GOLD TRANSACTIONAL OBT LOAD")
        print("=" * 100)

        # ====================================================
        # 1. Bootstrap ClickHouse
        # ====================================================

        bootstrap_gold()

        print("[PASS] Gold ClickHouse tables ensured.")

        # ====================================================
        # 2. Clear only staging
        #
        # Production remains untouched until validation passes.
        # ====================================================

        execute_clickhouse(
            f"""
            TRUNCATE TABLE
            {TRANSACTIONAL_OBT_STAGING}
            """
        )

        print("[PASS] Gold staging table cleared.")

        # ====================================================
        # 3. Read Silver Iceberg tables
        # ====================================================

        dim_date = spark.table(DIM_DATE)

        dim_user = spark.table(DIM_USER)

        dim_product = spark.table(DIM_PRODUCT)

        fact_order = spark.table(FACT_ORDER)

        fact_order_item = spark.table(FACT_ORDER_ITEM)

        # ====================================================
        # 4. Build OBT
        # ====================================================

        obt_df = build_transactional_obt(
            fact_order_item,
            fact_order,
            dim_user,
            dim_product,
            dim_date,
        ).cache()

        source_count = obt_df.count()

        distinct_items = obt_df.select("order_item_sk").distinct().count()

        duplicate_items = obt_df.groupBy("order_item_sk").count().filter(F.col("count") > 1).count()

        print()
        print("GOLD SOURCE AUDIT")
        print("-" * 100)

        print(f"Rows: {source_count:,}")

        print(f"Distinct order items: {distinct_items:,}")

        print(f"Duplicate order_item_sk: {duplicate_items:,}")

        if source_count != distinct_items or duplicate_items != 0:
            raise RuntimeError("Gold OBT source audit failed.")

        # ====================================================
        # 5. Write into ClickHouse Staging
        # ====================================================

        print()
        print("Writing Gold OBT to ClickHouse staging...")

        write_clickhouse(
            obt_df,
            TRANSACTIONAL_OBT_STAGING,
        )

        # ====================================================
        # 6. Validate Staging
        # ====================================================

        staging_df = read_clickhouse_table(
            spark,
            TRANSACTIONAL_OBT_STAGING,
        )

        staging_count = staging_df.count()

        print(f"Spark OBT rows: {source_count:,}")

        print(f"ClickHouse staging rows: {staging_count:,}")

        if staging_count != source_count:
            raise RuntimeError("Gold staging row count does not match Spark source.")

        print("[PASS] Gold staging validation passed.")

        # ====================================================
        # 7. Atomic Publish
        # ====================================================

        execute_clickhouse(
            f"""
            EXCHANGE TABLES
                {TRANSACTIONAL_OBT}
            AND
                {TRANSACTIONAL_OBT_STAGING}
            """
        )

        print("[PASS] Gold production table published.")

        # ====================================================
        # 8. Final Target Validation
        # ====================================================

        target_df = read_clickhouse_table(
            spark,
            TRANSACTIONAL_OBT,
        )

        target_count = target_df.count()

        if target_count != source_count:
            raise RuntimeError("Published Gold row count mismatch.")

        print()
        print(f"Final Gold rows: {target_count:,}")

        print()
        print("=" * 100)
        print("[PASS] GOLD TRANSACTIONAL OBT LOAD COMPLETED")
        print("=" * 100)

        obt_df.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
