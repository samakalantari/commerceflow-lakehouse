import os
from pyspark.sql import DataFrame

PARTITION_COLUMNS = (
    "year",
    "month",
    "day",
)


def _validate_partition_columns(df: DataFrame) -> None:
    missing_columns = [
        column
        for column in PARTITION_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        missing = ", ".join(missing_columns)

        raise ValueError(
            "Missing partition columns for Bronze write: "
            f"{missing}"
        )


def write_bronze_stream(df: DataFrame, topic: str, checkpoint_base: str):
    _validate_partition_columns(df)
    
    path = _topic_to_path(topic)
    checkpoint = f"{checkpoint_base}/{topic.replace('.', '/')}"

    return (
        df.writeStream
        .format("parquet")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .partitionBy(*PARTITION_COLUMNS)
        .outputMode("append")
        .start()
    )

def _topic_to_path(topic: str) -> str:
    base = os.environ["BRONZE_KAFKA_BASE_PATH"].rstrip("/")
    return f"{base}/{topic.replace('.', '/')}"

def _topic_to_checkpoint(checkpoint_base: str, topic: str,) -> str:
    base = checkpoint_base.rstrip("/")

    return (f"{base}/"f"{topic.replace('.', '/')}")