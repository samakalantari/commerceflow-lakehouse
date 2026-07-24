from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_apps.bronze.config.minio import (
    configure_minio_storage,
)

BRONZE_PATH = "s3a://commerceflow-lakehouse/bronze/behavioral/events/year=2026/month=7/day=17"


def main() -> None:
    spark = SparkSession.builder.appName("find-behavioral-time-ranges").getOrCreate()

    configure_minio_storage(spark)

    spark.sparkContext.setLogLevel("WARN")

    try:
        bronze_df = spark.read.parquet(BRONZE_PATH)

        (
            bronze_df.filter(F.col("ingested_at").isNotNull())
            .groupBy(
                F.window(
                    F.col("ingested_at"),
                    "10 minutes",
                )
            )
            .count()
            .select(
                F.col("window.start").alias("start_ts"),
                F.col("window.end").alias("end_ts"),
                F.col("count").alias("row_count"),
            )
            .orderBy("start_ts")
            .show(
                200,
                truncate=False,
            )
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
