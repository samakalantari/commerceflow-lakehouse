from unittest.mock import MagicMock, patch

from spark_apps.bronze.jobs.bronze_topic_job import build_stream


@patch("spark_apps.bronze.jobs.bronze_topic_job.write_bronze_stream")
@patch("spark_apps.bronze.jobs.bronze_topic_job.add_time_partitions")
@patch("spark_apps.bronze.jobs.bronze_topic_job.group_behavioral_event_fields")
@patch("spark_apps.bronze.jobs.bronze_topic_job.decode")
@patch("spark_apps.bronze.jobs.bronze_topic_job.read_kafka_stream")
@patch("spark_apps.bronze.jobs.bronze_topic_job.validate_topic")
def test_build_stream_builds_complete_pipeline(
    mock_validate_topic,
    mock_read_kafka_stream,
    mock_decode,
    mock_group_behavioral_event_fields,
    mock_add_time_partitions,
    mock_write_bronze_stream,
    monkeypatch,
):
    spark = MagicMock()

    raw_stream = MagicMock(name="raw_stream")

    decoded = MagicMock(name="decoded")

    transformed = MagicMock(name="transformed")

    partitioned = MagicMock(name="partitioned")

    query = MagicMock(name="streaming_query")

    monkeypatch.setenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092",
    )

    monkeypatch.setenv(
        "BRONZE_CHECKPOINT_BASE",
        "s3a://commerceflow-lakehouse/checkpoints/bronze",
    )

    mock_read_kafka_stream.return_value = raw_stream

    mock_decode.return_value = decoded

    mock_group_behavioral_event_fields.return_value = transformed

    mock_add_time_partitions.return_value = partitioned

    mock_write_bronze_stream.return_value = query

    result = build_stream(
        spark=spark,
        topic="behavioral.events",
    )

    mock_validate_topic.assert_called_once_with("behavioral.events")

    mock_read_kafka_stream.assert_called_once_with(
        spark=spark,
        bootstrap_servers="kafka:9092",
        topic="behavioral.events",
    )

    mock_decode.assert_called_once_with(
        raw_stream,
        "behavioral.events",
        payload_column="raw_value",
    )

    mock_group_behavioral_event_fields.assert_called_once_with(
        decoded,
        "behavioral.events",
    )

    mock_add_time_partitions.assert_called_once_with(
        transformed,
        "behavioral.events",
    )

    mock_write_bronze_stream.assert_called_once_with(
        partitioned,
        "behavioral.events",
        "s3a://commerceflow-lakehouse/checkpoints/bronze",
    )

    assert result is query


@patch("spark_apps.bronze.jobs.bronze_topic_job.write_bronze_stream")
@patch("spark_apps.bronze.jobs.bronze_topic_job.add_time_partitions")
@patch("spark_apps.bronze.jobs.bronze_topic_job.group_behavioral_event_fields")
@patch("spark_apps.bronze.jobs.bronze_topic_job.decode")
@patch("spark_apps.bronze.jobs.bronze_topic_job.read_kafka_stream")
@patch("spark_apps.bronze.jobs.bronze_topic_job.validate_topic")
def test_build_stream_applies_transform_to_transactional_topic(
    mock_validate_topic,
    mock_read_kafka_stream,
    mock_decode,
    mock_group_behavioral_event_fields,
    mock_add_time_partitions,
    mock_write_bronze_stream,
    monkeypatch,
):
    spark = MagicMock()

    raw_stream = MagicMock(name="raw_stream")

    decoded = MagicMock(name="decoded")

    partitioned = MagicMock(name="partitioned")

    query = MagicMock(name="streaming_query")

    monkeypatch.setenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092",
    )

    monkeypatch.setenv(
        "BRONZE_CHECKPOINT_BASE",
        "s3a://commerceflow-lakehouse/checkpoints/bronze",
    )

    mock_read_kafka_stream.return_value = raw_stream

    mock_decode.return_value = decoded

    # Non-behavioral topics are returned unchanged.
    mock_group_behavioral_event_fields.return_value = decoded

    mock_add_time_partitions.return_value = partitioned

    mock_write_bronze_stream.return_value = query

    result = build_stream(
        spark=spark,
        topic="transactional.orders",
    )

    mock_validate_topic.assert_called_once_with("transactional.orders")

    mock_read_kafka_stream.assert_called_once_with(
        spark=spark,
        bootstrap_servers="kafka:9092",
        topic="transactional.orders",
    )

    mock_decode.assert_called_once_with(
        raw_stream,
        "transactional.orders",
        payload_column="raw_value",
    )

    mock_group_behavioral_event_fields.assert_called_once_with(
        decoded,
        "transactional.orders",
    )

    mock_add_time_partitions.assert_called_once_with(
        decoded,
        "transactional.orders",
    )

    mock_write_bronze_stream.assert_called_once_with(
        partitioned,
        "transactional.orders",
        "s3a://commerceflow-lakehouse/checkpoints/bronze",
    )

    assert result is query


@patch("spark_apps.bronze.jobs.bronze_topic_job.write_bronze_stream")
@patch("spark_apps.bronze.jobs.bronze_topic_job.add_time_partitions")
@patch("spark_apps.bronze.jobs.bronze_topic_job.group_behavioral_event_fields")
@patch("spark_apps.bronze.jobs.bronze_topic_job.decode")
@patch("spark_apps.bronze.jobs.bronze_topic_job.read_kafka_stream")
@patch("spark_apps.bronze.jobs.bronze_topic_job.validate_topic")
def test_build_stream_returns_none_when_schema_is_missing(
    mock_validate_topic,
    mock_read_kafka_stream,
    mock_decode,
    mock_group_behavioral_event_fields,
    mock_add_time_partitions,
    mock_write_bronze_stream,
    monkeypatch,
    capsys,
):
    spark = MagicMock()

    raw_stream = MagicMock(name="raw_stream")

    monkeypatch.setenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092",
    )

    monkeypatch.setenv(
        "BRONZE_CHECKPOINT_BASE",
        "s3a://commerceflow-lakehouse/checkpoints/bronze",
    )

    mock_read_kafka_stream.return_value = raw_stream

    mock_decode.return_value = None

    result = build_stream(
        spark=spark,
        topic="transactional.returns_refunds",
    )

    assert result is None

    mock_validate_topic.assert_called_once_with("transactional.returns_refunds")

    mock_read_kafka_stream.assert_called_once_with(
        spark=spark,
        bootstrap_servers="kafka:9092",
        topic="transactional.returns_refunds",
    )

    mock_decode.assert_called_once_with(
        raw_stream,
        "transactional.returns_refunds",
        payload_column="raw_value",
    )

    mock_group_behavioral_event_fields.assert_not_called()
    mock_add_time_partitions.assert_not_called()
    mock_write_bronze_stream.assert_not_called()

    output = capsys.readouterr().out

    assert "No schema found for topic 'transactional.returns_refunds'" in output
