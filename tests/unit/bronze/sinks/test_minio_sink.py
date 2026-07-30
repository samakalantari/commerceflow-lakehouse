from unittest.mock import MagicMock

import pytest

from spark_apps.bronze.sinks.minio_sink import (
    _topic_to_checkpoint,
    _topic_to_path,
    _validate_partition_columns,
    write_bronze_stream,
)


BRONZE_V2_BASE = "s3a://commerceflow-lakehouse/bronze_v2"
CHECKPOINT_V2_BASE = "s3a://commerceflow-lakehouse/checkpoints/bronze_v2"


def _build_mock_dataframe(columns):
    df = MagicMock()
    writer = MagicMock()
    query = MagicMock()
    df.columns = columns
    df.writeStream = writer
    writer.foreachBatch.return_value = writer
    writer.option.return_value = writer
    writer.outputMode.return_value = writer
    writer.start.return_value = query
    return df, writer, query


def test_topic_path_uses_new_data_suffix(monkeypatch):
    monkeypatch.setenv("BRONZE_KAFKA_BASE_PATH", BRONZE_V2_BASE)

    assert _topic_to_path("transactional.orders") == (
        "s3a://commerceflow-lakehouse/bronze_v2/"
        "transactional/orders/new_data"
    )


def test_topic_checkpoint_uses_new_data_suffix():
    assert _topic_to_checkpoint(CHECKPOINT_V2_BASE, "transactional.users") == (
        "s3a://commerceflow-lakehouse/checkpoints/bronze_v2/"
        "transactional/users/new_data"
    )


def test_stream_uses_foreach_batch_and_ingested_at_checkpoint(monkeypatch):
    monkeypatch.setenv("BRONZE_KAFKA_BASE_PATH", BRONZE_V2_BASE)
    df, writer, query = _build_mock_dataframe(["order_id", "ingested_at"])

    result = write_bronze_stream(
        df=df,
        topic="transactional.orders",
        checkpoint_base=CHECKPOINT_V2_BASE,
    )

    writer.foreachBatch.assert_called_once()
    writer.option.assert_called_once_with(
        "checkpointLocation",
        "s3a://commerceflow-lakehouse/checkpoints/bronze_v2/"
        "transactional/orders/new_data",
    )
    writer.outputMode.assert_called_once_with("append")
    writer.start.assert_called_once_with()
    assert result is query


def test_stream_rejects_missing_ingested_at(monkeypatch):
    monkeypatch.setenv("BRONZE_KAFKA_BASE_PATH", BRONZE_V2_BASE)
    df, writer, _ = _build_mock_dataframe(["order_id"])

    with pytest.raises(ValueError, match="ingested_at"):
        write_bronze_stream(df, "transactional.orders", CHECKPOINT_V2_BASE)
