import os

from pyspark.sql import SparkSession

from bronze.config.minio import configure_minio_storage


def main() -> None:
    bucket = os.getenv(
        "MINIO_BUCKET",
        "commerceflow-lakehouse",
    )

    test_path = (
        f"s3a://{bucket}/"
        "_smoke_tests/spark_minio_connection"
    )

    spark = (
        SparkSession.builder
        .appName("MinIO-Storage-Smoke-Test")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    configure_minio_storage(spark)

    print(f"Writing test data to: {test_path}", flush=True)

    test_df = spark.createDataFrame(
        [
            (1, "minio-test"),
            (2, "spark-s3a-test"),
        ],
        schema=["id", "message"],
    )

    test_df.write.mode("overwrite").parquet(test_path)

    print("Reading test data from MinIO:", flush=True)

    result_df = spark.read.parquet(test_path)

    result_df.show(truncate=False)

    row_count = result_df.count()

    if row_count != 2:
        raise RuntimeError(
            f"Unexpected row count: {row_count}"
        )

    print(
        "Spark → MinIO write/read test passed.",
        flush=True,
    )

    spark.stop()


if __name__ == "__main__":
    main()