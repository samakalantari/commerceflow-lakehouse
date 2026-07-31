from pyspark.sql import functions as F

from spark_apps.silver.common.bronze_reader import (
    bronze_topic_day_path,
    read_bronze_topic,
    split_tombstones,
    apply_tombstone_deletes,
)
from spark_apps.silver.common.job_arguments import get_source_selection
from spark_apps.silver.config.iceberg import build_iceberg_spark
from spark_apps.silver.config.tables import (
    FACT_ORDER_ITEM,
    FACT_RETURN_REFUND,
    INVALID_RETURNS_REFUNDS,
    QUARANTINE_DATABASE,
    TOPIC_RETURNS_REFUNDS,
)
from spark_apps.silver.facts.fact_return_refund import build_fact_return_refund_source
from spark_apps.silver.quality.quarantine import (
    prepare_quarantine_records,
    write_quarantine,
)


def main() -> None:

    source = get_source_selection()
    spark = build_iceberg_spark("silver-load-fact-return-refund")

    try:
        print(f"Bronze source mode: {source.mode}")

        returns_df = read_bronze_topic(
            spark,
            TOPIC_RETURNS_REFUNDS,
            ingested_date=source.ingested_date,
            source_mode=source.mode,
        ).select(
            "return_refund_id", "order_id", "order_item_id",
            "return_reason", "return_timestamp", "refund_amount",
            "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset",
            "kafka_timestamp", "ingested_at",
        )
        returns_df, return_tombstones = split_tombstones(
            returns_df,
            business_key="return_refund_id",
            payload_columns=("return_refund_id", "order_id", "order_item_id", "return_reason", "return_timestamp", "refund_amount"),
        )
        fact_order_item_df = spark.table(FACT_ORDER_ITEM)

        source_df, invalid_df = build_fact_return_refund_source(
            returns_df,
            fact_order_item_df,
            persist_classified=True,
        )
        source_df = source_df.cache()

        source_count = source_df.count()
        distinct_returns = source_df.select("return_refund_id").distinct().count()
        duplicate_keys = (
            source_df.groupBy("return_refund_sk").count().filter(F.col("count") > 1).count()
        )
        missing_relationships = source_df.filter(
            F.col("order_sk").isNull() | F.col("order_item_sk").isNull()
        ).count()
        invalid_count = invalid_df.count()

        print("FACT_RETURN_REFUND SOURCE AUDIT")
        print(f"Source rows: {source_count:,}")
        print(f"Distinct returns/refunds: {distinct_returns:,}")
        print(f"Duplicate return_refund_sk: {duplicate_keys:,}")
        print(f"Missing resolved relationships: {missing_relationships:,}")
        print(f"Invalid returns/refunds: {invalid_count:,}")

        if (
            source_count != distinct_returns
            or duplicate_keys != 0
            or missing_relationships != 0
        ):
            raise RuntimeError("FACT_RETURN_REFUND canonical source audit failed.")

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {QUARANTINE_DATABASE}")
        quarantine_df = prepare_quarantine_records(
            invalid_df,
            entity_name="return_refund",
            source_topic=TOPIC_RETURNS_REFUNDS,
        )
        write_quarantine(quarantine_df, INVALID_RETURNS_REFUNDS)
        print(f"[INFO] Current returns/refunds in quarantine: {invalid_count:,}")

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {FACT_RETURN_REFUND}
            (
                return_refund_sk BIGINT,
                return_refund_id STRING,
                order_sk BIGINT,
                order_id STRING,
                order_item_sk BIGINT,
                order_item_id STRING,
                return_date_sk INT,
                return_timestamp TIMESTAMP,
                refund_amount DECIMAL(10,2),
                return_reason STRING,
                source_kafka_timestamp TIMESTAMP,
                silver_created_at TIMESTAMP,
                silver_updated_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(return_timestamp))
            TBLPROPERTIES ('format-version' = '2')
            """
        )

        if source.mode == "daily":
            apply_tombstone_deletes(
                spark, table_name=FACT_RETURN_REFUND, business_key="return_refund_id",
                tombstones=return_tombstones, view_name="deleted_returns"
            )

        write_df = (
            source_df.withColumn("silver_created_at", F.current_timestamp())
            .withColumn("silver_updated_at", F.current_timestamp())
        )
        write_df.createOrReplaceTempView("staged_fact_return_refund")
        spark.sql(
            f"""
            MERGE INTO {FACT_RETURN_REFUND} AS target
            USING staged_fact_return_refund AS source
            ON target.return_refund_id = source.return_refund_id
            WHEN MATCHED AND source.source_kafka_timestamp > target.source_kafka_timestamp
                THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )

        fact_df = spark.table(FACT_RETURN_REFUND)
        fact_count = fact_df.count()
        target_distinct = fact_df.select("return_refund_id").distinct().count()
        target_duplicates = (
            fact_df.groupBy("return_refund_sk").count().filter(F.col("count") > 1).count()
        )
        target_missing_relationships = fact_df.filter(
            F.col("order_sk").isNull() | F.col("order_item_sk").isNull()
        ).count()

        if (
            fact_count != target_distinct
            or target_duplicates != 0
            or target_missing_relationships != 0
        ):
            raise RuntimeError("FACT_RETURN_REFUND audit failed.")

        print("[PASS] FACT_RETURN_REFUND LOAD COMPLETED")
        source_df.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
