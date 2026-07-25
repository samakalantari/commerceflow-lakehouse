from unittest.mock import MagicMock

import pytest

from spark_apps.bronze.sinks.minio_sink import (
    _topic_to_checkpoint,
    _topic_to_path,
    _validate_partition_columns,
    write_bronze_stream,
)


BRONZE_BASE = "s3a://commerceflow-lakehouse/bronze"
CHECKPOINT_BASE = "s3a://commerceflow-lakehouse/checkpoints/bronze"

ORDERS_OUTPUT_PATH = (
    "s3a://commerceflow-lakehouse/"
    "bronze/transactional/orders_recovery"
)

ORDERS_CHECKPOINT_PATH = (
    "s3a://commerceflow-lakehouse/"
    "checkpoints/bronze/transactional/orders_recovery"
)

USERS_OUTPUT_PATH = (
    "s3a://commerceflow-lakehouse/"
    "bronze/transactional/users_recovery"
)

USERS_CHECKPOINT_PATH = (
    "s3a://commerceflow-lakehouse/"
    "checkpoints/bronze/transactional/users_recovery"
)


def _build_mock_dataframe(
    columns,
):
    df = MagicMock()
    writer = MagicMock()
    query = MagicMock()

    df.columns = columns
    df.writeStream = writer

    writer.format.return_value = writer
    writer.option.return_value = writer
    writer.outputMode.return_value = writer
    writer.partitionBy.return_value = writer
    writer.start.return_value = query

    return df, writer, query


def test_regular_topic_to_path_builds_expected_path(
    monkeypatch,
):
    monkeypatch.setenv(
        "BRONZE_KAFKA_BASE_PATH",
        BRONZE_BASE,
    )

    result = _topic_to_path(
        "transactional.products",
    )

    assert result == (
        "s3a://commerceflow-lakehouse/"
        "bronze/transactional/products"
    )


def test_orders_topic_uses_recovery_output_path(
    monkeypatch,
):
    monkeypatch.setenv(
        "BRONZE_KAFKA_BASE_PATH",
        BRONZE_BASE,
    )

    result = _topic_to_path(
        "transactional.orders",
    )

    assert result == ORDERS_OUTPUT_PATH


def test_users_topic_uses_recovery_output_path(
    monkeypatch,
):
    monkeypatch.setenv(
        "BRONZE_KAFKA_BASE_PATH",
        BRONZE_BASE,
    )

    result = _topic_to_path(
        "transactional.users",
    )

    assert result == USERS_OUTPUT_PATH


def test_topic_to_path_removes_trailing_slash(
    monkeypatch,
):
    monkeypatch.setenv(
        "BRONZE_KAFKA_BASE_PATH",
        f"{BRONZE_BASE}/",
    )

    result = _topic_to_path(
        "behavioral.events",
    )

    assert result == (
        "s3a://commerceflow-lakehouse/"
        "bronze/behavioral/events"
    )


def test_topic_to_path_uses_explicit_base_path(
    monkeypatch,
):
    monkeypatch.setenv(
        "BRONZE_KAFKA_BASE_PATH",
        "s3a://ignored-bucket/ignored",
    )

    result = _topic_to_path(
        topic="transactional.products",
        base_path="s3a://custom-bucket/custom-bronze",
    )

    assert result == (
        "s3a://custom-bucket/"
        "custom-bronze/transactional/products"
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
            "transactional.products",
        )


def test_regular_topic_to_checkpoint_builds_expected_path():
    result = _topic_to_checkpoint(
        checkpoint_base=CHECKPOINT_BASE,
        topic="transactional.products",
    )

    assert result == (
        "s3a://commerceflow-lakehouse/"
        "checkpoints/bronze/transactional/products"
    )


def test_orders_topic_uses_recovery_checkpoint_path():
    result = _topic_to_checkpoint(
        checkpoint_base=CHECKPOINT_BASE,
        topic="transactional.orders",
    )

    assert result == ORDERS_CHECKPOINT_PATH


def test_users_topic_uses_recovery_checkpoint_path():
    result = _topic_to_checkpoint(
        checkpoint_base=CHECKPOINT_BASE,
        topic="transactional.users",
    )

    assert result == USERS_CHECKPOINT_PATH


def test_topic_to_checkpoint_removes_trailing_slash():
    result = _topic_to_checkpoint(
        checkpoint_base=f"{CHECKPOINT_BASE}/",
        topic="behavioral.events",
    )

    assert result == (
        "s3a://commerceflow-lakehouse/"
        "checkpoints/bronze/behavioral/events"
    )


def test_validate_partition_columns_accepts_no_partitions():
    df = MagicMock()
    df.columns = [
        "category_id",
        "name",
    ]

    _validate_partition_columns(
        df=df,
        partition_columns=(),
    )


def test_validate_partition_columns_accepts_existing_columns():
    df = MagicMock()
    df.columns = [
        "order_id",
        "year",
        "month",
        "day",
    ]

    _validate_partition_columns(
        df=df,
        partition_columns=(
            "year",
            "month",
            "day",
        ),
    )


def test_validate_partition_columns_rejects_missing_columns():
    df = MagicMock()
    df.columns = [
        "order_id",
        "year",
    ]

    with pytest.raises(
        ValueError,
        match="Missing partition columns",
    ) as exc_info:
        _validate_partition_columns(
            df=df,
            partition_columns=(
                "year",
                "month",
                "day",
            ),
        )

    error_message = str(exc_info.value)

    assert "month" in error_message
    assert "day" in error_message
    assert "year" not in error_message


def test_write_categories_without_partitioning(
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

    writer.partitionBy.assert_not_called()
    writer.start.assert_called_once_with()

    assert result is query


def test_write_orders_uses_recovery_paths_and_time_partitioning(
    monkeypatch,
):
    monkeypatch.setenv(
        "BRONZE_KAFKA_BASE_PATH",
        BRONZE_BASE,
    )

    df, writer, query = _build_mock_dataframe(
        columns=[
            "order_id",
            "timestamp",
            "year",
            "month",
            "day",
        ],
    )

    result = write_bronze_stream(
        df=df,
        topic="transactional.orders",
        checkpoint_base=CHECKPOINT_BASE,
    )

    writer.format.assert_called_once_with(
        "parquet",
    )

    writer.option.assert_any_call(
        "path",
        ORDERS_OUTPUT_PATH,
    )

    writer.option.assert_any_call(
        "checkpointLocation",
        ORDERS_CHECKPOINT_PATH,
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


def test_write_regular_partitioned_topic_uses_standard_paths(
    monkeypatch,
):
    monkeypatch.setenv(
        "BRONZE_KAFKA_BASE_PATH",
        BRONZE_BASE,
    )

    df, writer, query = _build_mock_dataframe(
        columns=[
            "product_id",
            "ingested_at",
            "year",
            "month",
            "day",
        ],
    )

    result = write_bronze_stream(
        df=df,
        topic="transactional.products",
        checkpoint_base=CHECKPOINT_BASE,
    )

    writer.option.assert_any_call(
        "path",
        (
            "s3a://commerceflow-lakehouse/"
            "bronze/transactional/products"
        ),
    )

    writer.option.assert_any_call(
        "checkpointLocation",
        (
            "s3a://commerceflow-lakehouse/"
            "checkpoints/bronze/"
            "transactional/products"
        ),
    )

    writer.partitionBy.assert_called_once_with(
        "year",
        "month",
        "day",
    )

    writer.start.assert_called_once_with()

    assert result is query


def test_write_partitioned_topic_rejects_missing_columns(
    monkeypatch,
):
    monkeypatch.setenv(
        "BRONZE_KAFKA_BASE_PATH",
        BRONZE_BASE,
    )

    df, writer, _ = _build_mock_dataframe(
        columns=[
            "order_id",
            "year",
        ],
    )

    with pytest.raises(
        ValueError,
        match="Missing partition columns",
    ):
        write_bronze_stream(
            df=df,
            topic="transactional.orders",
            checkpoint_base=CHECKPOINT_BASE,
        )

    writer.start.assert_not_called()


def test_write_unknown_topic_raises(
    monkeypatch,
):
    monkeypatch.setenv(
        "BRONZE_KAFKA_BASE_PATH",
        BRONZE_BASE,
    )

    df, writer, _ = _build_mock_dataframe(
        columns=[],
    )

    with pytest.raises(
        ValueError,
        match="No partition configuration",
    ):
        write_bronze_stream(
            df=df,
            topic="transactional.unknown",
            checkpoint_base=CHECKPOINT_BASE,
        )

    writer.start.assert_not_called()