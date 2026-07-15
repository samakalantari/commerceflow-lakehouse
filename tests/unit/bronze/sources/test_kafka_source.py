from unittest.mock import MagicMock, patch

import pytest

from spark_apps.bronze.sources.kafka_source import (
    read_kafka_stream,
)


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
    "spark_apps.bronze.sources.kafka_source.current_timestamp"
)
@patch(
    "spark_apps.bronze.sources.kafka_source.col"
)
def test_read_kafka_stream_configures_kafka_reader(
    mock_col,
    mock_current_timestamp,
):
    spark = MagicMock()
    reader = MagicMock()
    loaded_df = MagicMock()
    selected_df = MagicMock()

    spark.readStream = reader

    reader.format.return_value = reader
    reader.option.return_value = reader
    reader.load.return_value = loaded_df
    loaded_df.select.return_value = selected_df

    mock_column = MagicMock()
    mock_col.return_value = mock_column
    mock_column.cast.return_value = mock_column
    mock_column.alias.return_value = mock_column

    mock_ingested_at = MagicMock()
    mock_current_timestamp.return_value = mock_ingested_at
    mock_ingested_at.alias.return_value = mock_ingested_at

    result = read_kafka_stream(
        spark=spark,
        bootstrap_servers="kafka:9092",
        topic="transactional.orders",
        starting_offsets="latest",
        max_offsets_per_trigger=500,
    )

    reader.format.assert_called_once_with("kafka")

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
        "true",
    )
    reader.option.assert_any_call(
        "maxOffsetsPerTrigger",
        "500",
    )

    reader.load.assert_called_once_with()
    loaded_df.select.assert_called_once()

    mock_col.assert_any_call("key")
    mock_col.assert_any_call("value")
    mock_col.assert_any_call("topic")
    mock_col.assert_any_call("partition")
    mock_col.assert_any_call("offset")
    mock_col.assert_any_call("timestamp")

    mock_current_timestamp.assert_called_once_with()

    assert result is selected_df


@patch(
    "spark_apps.bronze.sources.kafka_source.current_timestamp"
)
@patch(
    "spark_apps.bronze.sources.kafka_source.col"
)
def test_read_kafka_stream_omits_max_offsets_when_none(
    mock_col,
    mock_current_timestamp,
):
    spark = MagicMock()
    reader = MagicMock()
    loaded_df = MagicMock()

    spark.readStream = reader

    reader.format.return_value = reader
    reader.option.return_value = reader
    reader.load.return_value = loaded_df
    loaded_df.select.return_value = MagicMock()

    mock_column = MagicMock()
    mock_col.return_value = mock_column
    mock_column.cast.return_value = mock_column
    mock_column.alias.return_value = mock_column

    mock_ingested_at = MagicMock()
    mock_current_timestamp.return_value = mock_ingested_at
    mock_ingested_at.alias.return_value = mock_ingested_at

    read_kafka_stream(
        spark=spark,
        bootstrap_servers="kafka:9092",
        topic="behavioral.events",
        max_offsets_per_trigger=None,
    )

    configured_options = [
        call.args
        for call in reader.option.call_args_list
    ]

    assert not any(
        option_name == "maxOffsetsPerTrigger"
        for option_name, _ in configured_options
    )