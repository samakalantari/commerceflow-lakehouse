from pyspark.sql import functions as F

from spark_apps.silver.common.bronze_reader import (
    bronze_topic_paths,
    read_bronze_topic,
)
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
    spark = build_iceberg_spark("silver-load-fact-return-refund")

    try:
        print("Bronze input paths:")
        for path in bronze_topic_paths(TOPIC_RETURNS_REFUNDS):
            print(f"  - {path}/year=*/month=*/day=*")

        returns_df = read_bronze_topic(
            spark,
            TOPIC_RETURNS_REFUNDS,
        ).select(
            "return_refund_id", "order_id", "order_item_id",
            "return_reason", "return_timestamp", "refund_amount",
            "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset",
            "kafka_timestamp", "ingested_at", "year", "month", "day",
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
        if invalid_count > 0:
            quarantine_df = prepare_quarantine_records(
                invalid_df,
                entity_name="return_refund",
                source_topic=TOPIC_RETURNS_REFUNDS,
            )
            write_quarantine(quarantine_df, INVALID_RETURNS_REFUNDS)
            print(f"[WARN] {invalid_count:,} returns/refunds written to quarantine.")

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

        write_df = (
            source_df.withColumn("silver_created_at", F.current_timestamp())
            .withColumn("silver_updated_at", F.current_timestamp())
        )
        write_df.writeTo(FACT_RETURN_REFUND).overwrite(F.lit(True))

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
