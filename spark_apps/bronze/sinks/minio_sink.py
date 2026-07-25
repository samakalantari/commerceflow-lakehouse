import os
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from spark_apps.bronze.config.topic_metadata import (
    get_partition_columns,
)

TOPIC_PATH_OVERRIDES = {
    "transactional.orders": "transactional/orders_recovery",
    "transactional.product_price_history": "transactional/product_price_history_recovery",
    "transactional.users": "transactional/users_recovery",
}

TOPIC_CHECKPOINT_OVERRIDES = {
    "transactional.orders": "transactional/orders_recovery",
    "transactional.product_price_history": "transactional/product_price_history_recovery",
    "transactional.users": "transactional/users_recovery",
}


def _validate_partition_columns(
    df: DataFrame,
    partition_columns: tuple[str, ...],
) -> None:

    if not partition_columns:
        return

    missing_columns = [column for column in partition_columns if column not in df.columns]

    if missing_columns:
        missing = ", ".join(missing_columns)

        raise ValueError(f"Missing partition columns for Bronze write: {missing}")


def write_bronze_stream(df: DataFrame, topic: str, checkpoint_base: str):
    partition_columns = get_partition_columns(topic)

    _validate_partition_columns(df=df, partition_columns=partition_columns)

    path = _topic_to_path(topic=topic)

    checkpoint = _topic_to_checkpoint(checkpoint_base=checkpoint_base, topic=topic)

    writer = (
        df.writeStream.format("parquet")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
    )

    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    return writer.start()


def write_bronze_batch(
    df: DataFrame, topic: str, output_base_path: str, mode: str = "errorifexists"
) -> str:
    partition_columns = get_partition_columns(topic)

    _validate_partition_columns(df=df, partition_columns=partition_columns)

    output_path = _topic_to_path(topic=topic, base_path=output_base_path)

    writer = df.write.format("parquet").mode(mode)

    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    writer.save(output_path)

    return output_path


def _topic_to_path(
    topic: str,
    base_path: Optional[str] = None,
) -> str:
    if base_path is None:
        base_path = os.environ["BRONZE_KAFKA_BASE_PATH"]

    base = base_path.rstrip("/")

    topic_path = TOPIC_PATH_OVERRIDES.get(
        topic,
        topic.replace(".", "/"),
    )

    return f"{base}/{topic_path}"


def _topic_to_checkpoint(
    checkpoint_base: str,
    topic: str,
) -> str:
    base = checkpoint_base.rstrip("/")

    topic_path = TOPIC_CHECKPOINT_OVERRIDES.get(
        topic,
        topic.replace(".", "/"),
    )

    return f"{base}/{topic_path}"

def test_write_categories_with_ingestion_time_partitioning(
    monkeypatch,
):
    monkeypatch.setenv(
        "BRONZE_KAFKA_BASE_PATH",
        BRONZE_BASE,
    )

    df, writer, query = _build_mock_dataframe(
        columns=[
            "category_id",
            "name",
            "parent_category_id",
            "ingested_at",
            "year",
            "month",
            "day",
        ],
    )

    result = write_bronze_stream(
        df=df,
        topic="transactional.categories",
        checkpoint_base=CHECKPOINT_BASE,
    )

    writer.format.assert_called_once_with(
        "parquet",
    )

    writer.option.assert_any_call(
        "path",
        (
            "s3a://commerceflow-lakehouse/"
            "bronze/transactional/categories"
        ),
    )

    writer.option.assert_any_call(
        "checkpointLocation",
        (
            "s3a://commerceflow-lakehouse/"
            "checkpoints/bronze/"
            "transactional/categories"
        ),
    )

    writer.outputMode.assert_called_once_with(
        "append",
    )

    writer.partitionBy.assert_called_once_with(
        "year",
        "month",
        "day",
    )

    writer.start.assert_called_once_with()

    assert result is query