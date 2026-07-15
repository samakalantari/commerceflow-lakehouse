from unittest.mock import MagicMock

import pytest

from spark_apps.bronze.sinks.minio_sink import (
    PARTITION_COLUMNS,
    _topic_to_checkpoint,
    _topic_to_path,
    write_bronze_stream,
)


def test_topic_to_path_builds_expected_path(
    monkeypatch,
):
    monkeypatch.setenv(
        "BRONZE_KAFKA_BASE_PATH",
        "s3a://commerceflow-lakehouse/bronze",
    )

    result = _topic_to_path(
        "transactional.orders"
    )

    assert result == (
        "s3a://commerceflow-lakehouse/"
        "bronze/transactional/orders"
    )


def test_topic_to_path_removes_trailing_slash(
    monkeypatch,
):
    monkeypatch.setenv(
        "BRONZE_KAFKA_BASE_PATH",
        "s3a://commerceflow-lakehouse/bronze/",
    )

    result = _topic_to_path(
        "behavioral.events"
    )

    assert result == (
        "s3a://commerceflow-lakehouse/"
        "bronze/behavioral/events"
    )


def test_topic_to_path_raises_when_base_path_missing(
    monkeypatch,
):
    monkeypatch.delenv(
        "BRONZE_KAFKA_BASE_PATH",
        raising=False,
    )

    with pytest.raises(KeyError):
        _topic_to_path(
            "transactional.orders"
        )


def test_topic_to_checkpoint_builds_expected_path():
    result = _topic_to_checkpoint(
        (
            "s3a://commerceflow-lakehouse/"
            "checkpoints/bronze"
        ),
        "transactional.orders",
    )

    assert result == (
        "s3a://commerceflow-lakehouse/"
        "checkpoints/bronze/"
        "transactional/orders"
    )


def test_topic_to_checkpoint_removes_trailing_slash():
    result = _topic_to_checkpoint(
        (
            "s3a://commerceflow-lakehouse/"
            "checkpoints/bronze/"
        ),
        "behavioral.events",
    )

    assert result == (
        "s3a://commerceflow-lakehouse/"
        "checkpoints/bronze/"
        "behavioral/events"
    )


def test_write_bronze_stream_rejects_missing_partition_columns(
    monkeypatch,
):
    monkeypatch.setenv(
        "BRONZE_KAFKA_BASE_PATH",
        "s3a://commerceflow-lakehouse/bronze",
    )

    df = MagicMock()
    df.columns = [
        "order_id",
        "year",
    ]

    with pytest.raises(
        ValueError,
        match=(
            "Missing partition columns "
            "for Bronze write"
        ),
    ):
        write_bronze_stream(
            df=df,
            topic="transactional.orders",
            checkpoint_base=(
                "s3a://commerceflow-lakehouse/"
                "checkpoints/bronze"
            ),
        )


def test_write_bronze_stream_lists_missing_columns(
    monkeypatch,
):
    monkeypatch.setenv(
        "BRONZE_KAFKA_BASE_PATH",
        "s3a://commerceflow-lakehouse/bronze",
    )

    df = MagicMock()
    df.columns = [
        "order_id",
        "year",
    ]

    with pytest.raises(
        ValueError,
    ) as exc_info:
        write_bronze_stream(
            df=df,
            topic="transactional.orders",
            checkpoint_base=(
                "s3a://commerceflow-lakehouse/"
                "checkpoints/bronze"
            ),
        )

    error_message = str(
        exc_info.value
    )

    assert "month" in error_message
    assert "day" in error_message
    assert "year" not in error_message


def test_write_bronze_stream_configures_writer(
    monkeypatch,
):
    monkeypatch.setenv(
        "BRONZE_KAFKA_BASE_PATH",
        "s3a://commerceflow-lakehouse/bronze",
    )

    df = MagicMock()
    writer = MagicMock()
    query = MagicMock()

    df.columns = [
        "order_id",
        "year",
        "month",
        "day",
    ]

    df.writeStream = writer

    writer.format.return_value = writer
    writer.option.return_value = writer
    writer.partitionBy.return_value = writer
    writer.outputMode.return_value = writer
    writer.start.return_value = query

    result = write_bronze_stream(
        df=df,
        topic="transactional.orders",
        checkpoint_base=(
            "s3a://commerceflow-lakehouse/"
            "checkpoints/bronze"
        ),
    )

    writer.format.assert_called_once_with(
        "parquet"
    )

    writer.option.assert_any_call(
        "path",
        (
            "s3a://commerceflow-lakehouse/"
            "bronze/transactional/orders"
        ),
    )

    writer.option.assert_any_call(
        "checkpointLocation",
        (
            "s3a://commerceflow-lakehouse/"
            "checkpoints/bronze/"
            "transactional/orders"
        ),
    )

    writer.partitionBy.assert_called_once_with(
        *PARTITION_COLUMNS
    )

    writer.outputMode.assert_called_once_with(
        "append"
    )

    writer.start.assert_called_once_with()

    assert result is query