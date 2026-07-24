from unittest.mock import MagicMock, patch

import pytest

from spark_apps.bronze.sources.kafka_source import (
    read_kafka_stream,
)


def _configure_mock_spark():
    spark = MagicMock()
    reader = MagicMock()
    loaded_df = MagicMock()
    selected_df = MagicMock()

    spark.readStream = reader

    reader.format.return_value = reader
    reader.option.return_value = reader
    reader.load.return_value = loaded_df

    loaded_df.select.return_value = selected_df

    return spark, reader, loaded_df, selected_df


def _configure_mock_columns(
    mock_col,
    mock_current_timestamp,
):
    mock_column = MagicMock()

    mock_col.return_value = mock_column
    mock_column.cast.return_value = mock_column
    mock_column.alias.return_value = mock_column

    mock_ingested_at = MagicMock()
    mock_current_timestamp.return_value = mock_ingested_at
    mock_ingested_at.alias.return_value = mock_ingested_at

    return mock_column, mock_ingested_at


def test_read_kafka_stream_rejects_empty_bootstrap_servers():
    spark = MagicMock()

    with pytest.raises(
        ValueError,
        match="bootstrap_servers must not be empty",
    ):
        read_kafka_stream(
            spark=spark,
            bootstrap_servers="",
            topic="transactional.orders",
        )


def test_read_kafka_stream_rejects_empty_topic():
    spark = MagicMock()

    with pytest.raises(
        ValueError,
        match="topic must not be empty",
    ):
        read_kafka_stream(
            spark=spark,
            bootstrap_servers="kafka:9092",
            topic="",
        )


@patch(
    "spark_apps.bronze.sources."
    "kafka_source.current_timestamp"
)
@patch(
    "spark_apps.bronze.sources."
    "kafka_source.col"
)
def test_read_kafka_stream_configures_kafka_reader(
    mock_col,
    mock_current_timestamp,
):
    spark, reader, loaded_df, selected_df = (
        _configure_mock_spark()
    )

    _configure_mock_columns(
        mock_col=mock_col,
        mock_current_timestamp=mock_current_timestamp,
    )

    result = read_kafka_stream(
        spark=spark,
        bootstrap_servers="kafka:9092",
        topic="transactional.orders",
        starting_offsets="latest",
        max_offsets_per_trigger=500,
    )

    reader.format.assert_called_once_with(
        "kafka",
    )

    reader.option.assert_any_call(
        "kafka.bootstrap.servers",
        "kafka:9092",
    )

    reader.option.assert_any_call(
        "subscribe",
        "transactional.orders",
    )

    reader.option.assert_any_call(
        "startingOffsets",
        "latest",
    )

    reader.option.assert_any_call(
        "failOnDataLoss",
        "false",
    )

    reader.option.assert_any_call(
        "maxOffsetsPerTrigger",
        "500",
    )

    reader.load.assert_called_once_with()
    loaded_df.select.assert_called_once()

    mock_col.assert_any_call(
        "key",
    )
    mock_col.assert_any_call(
        "value",
    )
    mock_col.assert_any_call(
        "topic",
    )
    mock_col.assert_any_call(
        "partition",
    )
    mock_col.assert_any_call(
        "offset",
    )
    mock_col.assert_any_call(
        "timestamp",
    )

    mock_current_timestamp.assert_called_once_with()

    assert result is selected_df


@patch(
    "spark_apps.bronze.sources."
    "kafka_source.current_timestamp"
)
@patch(
    "spark_apps.bronze.sources."
    "kafka_source.col"
)
def test_read_kafka_stream_uses_default_starting_offsets(
    mock_col,
    mock_current_timestamp,
):
    spark, reader, _, _ = _configure_mock_spark()

    _configure_mock_columns(
        mock_col=mock_col,
        mock_current_timestamp=mock_current_timestamp,
    )

    read_kafka_stream(
        spark=spark,
        bootstrap_servers="kafka:9092",
        topic="transactional.products",
        max_offsets_per_trigger=1000,
    )

    reader.option.assert_any_call(
        "startingOffsets",
        "earliest",
    )


@patch(
    "spark_apps.bronze.sources."
    "kafka_source.current_timestamp"
)
@patch(
    "spark_apps.bronze.sources."
    "kafka_source.col"
)
def test_read_kafka_stream_disables_failure_on_data_loss(
    mock_col,
    mock_current_timestamp,
):
    spark, reader, _, _ = _configure_mock_spark()

    _configure_mock_columns(
        mock_col=mock_col,
        mock_current_timestamp=mock_current_timestamp,
    )

    read_kafka_stream(
        spark=spark,
        bootstrap_servers="kafka:9092",
        topic="behavioral.events",
    )

    reader.option.assert_any_call(
        "failOnDataLoss",
        "false",
    )


@patch(
    "spark_apps.bronze.sources."
    "kafka_source.current_timestamp"
)
@patch(
    "spark_apps.bronze.sources."
    "kafka_source.col"
)
def test_read_kafka_stream_omits_max_offsets_when_none(
    mock_col,
    mock_current_timestamp,
):
    spark, reader, _, _ = _configure_mock_spark()

    _configure_mock_columns(
        mock_col=mock_col,
        mock_current_timestamp=mock_current_timestamp,
    )

    read_kafka_stream(
        spark=spark,
        bootstrap_servers="kafka:9092",
        topic="behavioral.events",
        max_offsets_per_trigger=None,
    )

    configured_options = [
        current_call.args
        for current_call in reader.option.call_args_list
    ]

    assert not any(
        option_name == "maxOffsetsPerTrigger"
        for option_name, _ in configured_options
    )


@patch(
    "spark_apps.bronze.sources."
    "kafka_source.current_timestamp"
)
@patch(
    "spark_apps.bronze.sources."
    "kafka_source.col"
)
def test_read_kafka_stream_converts_max_offsets_to_string(
    mock_col,
    mock_current_timestamp,
):
    spark, reader, _, _ = _configure_mock_spark()

    _configure_mock_columns(
        mock_col=mock_col,
        mock_current_timestamp=mock_current_timestamp,
    )

    read_kafka_stream(
        spark=spark,
        bootstrap_servers="kafka:9092",
        topic="transactional.products",
        max_offsets_per_trigger=2500,
    )

    reader.option.assert_any_call(
        "maxOffsetsPerTrigger",
        "2500",
    )

    assert (
        "maxOffsetsPerTrigger",
        2500,
    ) not in [
        current_call.args
        for current_call in reader.option.call_args_list
    ]


@patch(
    "spark_apps.bronze.sources."
    "kafka_source.current_timestamp"
)
@patch(
    "spark_apps.bronze.sources."
    "kafka_source.col"
)
def test_read_kafka_stream_selects_expected_metadata_columns(
    mock_col,
    mock_current_timestamp,
):
    spark, _, loaded_df, _ = _configure_mock_spark()

    mock_column, mock_ingested_at = (
        _configure_mock_columns(
            mock_col=mock_col,
            mock_current_timestamp=mock_current_timestamp,
        )
    )

    read_kafka_stream(
        spark=spark,
        bootstrap_servers="kafka:9092",
        topic="transactional.orders",
    )

    loaded_df.select.assert_called_once_with(
        mock_column,
        mock_column,
        mock_column,
        mock_column,
        mock_column,
        mock_column,
        mock_ingested_at,
    )