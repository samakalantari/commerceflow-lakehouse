import os

from pyspark.sql import DataFrame, SparkSession


def get_bronze_base_path() -> str:
    """
    Return the Bronze Kafka base path stored in MinIO.
    """
    base_path = os.getenv("BRONZE_KAFKA_BASE_PATH")

    if not base_path:
        raise RuntimeError("BRONZE_KAFKA_BASE_PATH is not set.")

    return base_path.rstrip("/")


def bronze_topic_path(
    topic: str,
) -> str:
    """
    Convert a Kafka topic into its Bronze MinIO path.

    Example:
        transactional.orders
        ->
        s3a://commerceflow-lakehouse/
        bronze/transactional/orders
    """
    base_path = get_bronze_base_path()

    topic_path = topic.replace(
        ".",
        "/",
    )

    return f"{base_path}/{topic_path}"


def read_bronze_topic(
    spark: SparkSession,
    topic: str,
) -> DataFrame:
    """
    Read a Bronze Kafka topic stored as Parquet.
    """
    path = bronze_topic_path(topic)

    return spark.read.option(
        "basePath",
        path,
    ).parquet(f"{path}/year=*/month=*/day=*")
