import os
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


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
    _validate_partition_columns(df=df, partition_columns=("ingested_at",))
    base_path = os.environ["BRONZE_KAFKA_BASE_PATH"]
    checkpoint = _topic_to_checkpoint(checkpoint_base=checkpoint_base, topic=topic)
    return (
        df.writeStream.foreachBatch(
            lambda batch_df, _: write_bronze_batch(batch_df, topic, base_path, "append")
        )
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .start()
    )


def write_bronze_batch(
    df: DataFrame, topic: str, output_base_path: str, mode: str = "errorifexists"
) -> str:
    _validate_partition_columns(df=df, partition_columns=("ingested_at",))
    output_path = _topic_to_path(topic=topic, base_path=output_base_path)
    dated = df.withColumn("_bronze_ingested_date", F.to_date("ingested_at"))
    invalid = dated.filter(F.col("_bronze_ingested_date").isNull()).count()
    if invalid:
        raise ValueError(f"{invalid} rows have no valid ingested_at date")

    dates = (
        dated.select("_bronze_ingested_date")
        .distinct()
        .orderBy("_bronze_ingested_date")
        .collect()
    )
    for row in dates:
        ingested_date = row["_bronze_ingested_date"]
        date_path = (
            f"{output_path}/{ingested_date.year:04d}/"
            f"{ingested_date.month:02d}/{ingested_date.day:02d}"
        )
        (
            dated.filter(F.col("_bronze_ingested_date") == F.lit(ingested_date))
            .drop("_bronze_ingested_date")
            .write.format("parquet")
            .mode(mode)
            .save(date_path)
        )

    return output_path


def _topic_to_path(
    topic: str,
    base_path: Optional[str] = None,
) -> str:
    if base_path is None:
        base_path = os.environ["BRONZE_KAFKA_BASE_PATH"]

    base = base_path.rstrip("/")

    return f"{base}/{topic.replace('.', '/')}/new_data"


def _topic_to_checkpoint(
    checkpoint_base: str,
    topic: str,
) -> str:
    base = checkpoint_base.rstrip("/")

    return f"{base}/{topic.replace('.', '/')}/new_data"

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