from unittest.mock import MagicMock, call, patch

import pytest

from spark_apps.bronze.jobs.bronze_topic_job import (
    build_spark,
    build_stream,
    main,
)

# Test build spark
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "configure_minio_storage"
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "SparkSession"
)
def test_build_spark_creates_session_and_configures_minio(
    mock_spark_session,
    mock_configure_minio,
):
    spark = MagicMock()

    builder = (
        mock_spark_session
        .builder
        .appName
        .return_value
    )
    builder.getOrCreate.return_value = spark

    result = build_spark(
        app_name="test-bronze-job"
    )

    mock_spark_session.builder.appName.assert_called_once_with(
        "test-bronze-job"
    )
    builder.getOrCreate.assert_called_once_with()

    mock_configure_minio.assert_called_once_with(
        spark
    )

    assert result is spark
    
# Test build stream
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "write_bronze_stream"
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "add_time_partitions"
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "decode"
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "read_kafka_stream"
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "validate_topic"
)
def test_build_stream_builds_complete_pipeline(
    mock_validate_topic,
    mock_read_kafka_stream,
    mock_decode,
    mock_add_time_partitions,
    mock_write_bronze_stream,
    monkeypatch,
):
    spark = MagicMock()
    raw_stream = MagicMock()
    decoded = MagicMock()
    partitioned = MagicMock()
    query = MagicMock()

    monkeypatch.setenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092",
    )
    monkeypatch.setenv(
        "BRONZE_CHECKPOINT_BASE",
        "s3a://lakehouse/checkpoints/bronze",
    )

    mock_read_kafka_stream.return_value = raw_stream
    mock_decode.return_value = decoded
    mock_add_time_partitions.return_value = (
        partitioned
    )
    mock_write_bronze_stream.return_value = query

    result = build_stream(
        spark,
        "transactional.orders",
    )

    mock_validate_topic.assert_called_once_with(
        "transactional.orders"
    )

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

    mock_add_time_partitions.assert_called_once_with(
        decoded,
        "transactional.orders",
    )

    mock_write_bronze_stream.assert_called_once_with(
        partitioned,
        "transactional.orders",
        "s3a://lakehouse/checkpoints/bronze",
    )

    assert result is query
    
# Test schema not found
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "write_bronze_stream"
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "add_time_partitions"
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "decode"
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "read_kafka_stream"
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "validate_topic"
)
def test_build_stream_returns_none_when_schema_is_missing(
    mock_validate_topic,
    mock_read_kafka_stream,
    mock_decode,
    mock_add_time_partitions,
    mock_write_bronze_stream,
    monkeypatch,
    capsys,
):
    spark = MagicMock()
    raw_stream = MagicMock()

    monkeypatch.setenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092",
    )
    monkeypatch.setenv(
        "BRONZE_CHECKPOINT_BASE",
        "/checkpoints",
    )

    mock_read_kafka_stream.return_value = raw_stream
    mock_decode.return_value = None

    result = build_stream(
        spark,
        "transactional.returns_refunds",
    )

    assert result is None

    mock_add_time_partitions.assert_not_called()
    mock_write_bronze_stream.assert_not_called()

    output = capsys.readouterr().out

    assert (
        "No schema found for topic "
        "'transactional.returns_refunds'"
        in output
    )

# Test Topic not valid
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "read_kafka_stream"
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "validate_topic"
)
def test_build_stream_stops_when_topic_is_invalid(
    mock_validate_topic,
    mock_read_kafka_stream,
):
    spark = MagicMock()

    mock_validate_topic.side_effect = ValueError(
        "Unsupported Kafka topic"
    )

    with pytest.raises(
        ValueError,
        match="Unsupported Kafka topic",
    ):
        build_stream(
            spark,
            "invalid.topic",
        )

    mock_read_kafka_stream.assert_not_called()

# Test missing kafka env not found
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "validate_topic"
)
def test_build_stream_requires_kafka_bootstrap_servers(
    mock_validate_topic,
    monkeypatch,
):
    spark = MagicMock()

    monkeypatch.delenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        raising=False,
    )
    monkeypatch.setenv(
        "BRONZE_CHECKPOINT_BASE",
        "/checkpoints",
    )

    with pytest.raises(
        KeyError,
        match="KAFKA_BOOTSTRAP_SERVERS",
    ):
        build_stream(
            spark,
            "transactional.orders",
        )


# Test checkpoint envs not found
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "validate_topic"
)
def test_build_stream_requires_checkpoint_base(
    mock_validate_topic,
    monkeypatch,
):
    spark = MagicMock()

    monkeypatch.setenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092",
    )
    monkeypatch.delenv(
        "BRONZE_CHECKPOINT_BASE",
        raising=False,
    )

    with pytest.raises(
        KeyError,
        match="BRONZE_CHECKPOINT_BASE",
    ):
        build_stream(
            spark,
            "transactional.orders",
        )

# Test Streams 
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "BUSINESS_TOPICS",
    (
        "transactional.orders",
        "behavioral.events",
    ),
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "build_stream"
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "build_spark"
)
def test_main_starts_all_topics_and_waits(
    mock_build_spark,
    mock_build_stream,
):
    spark = MagicMock()

    first_query = MagicMock()
    second_query = MagicMock()

    mock_build_spark.return_value = spark
    mock_build_stream.side_effect = [
        first_query,
        second_query,
    ]

    main()

    assert mock_build_stream.call_args_list == [
        call(
            spark,
            "transactional.orders",
        ),
        call(
            spark,
            "behavioral.events",
        ),
    ]

    spark.streams.awaitAnyTermination.assert_called_once_with()


@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "BUSINESS_TOPICS",
    (
        "transactional.orders",
        "behavioral.events",
    ),
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "build_stream"
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "build_spark"
)
def test_main_continues_when_one_topic_fails(
    mock_build_spark,
    mock_build_stream,
    capsys,
):
    spark = MagicMock()
    successful_query = MagicMock()

    mock_build_spark.return_value = spark
    mock_build_stream.side_effect = [
        RuntimeError("Kafka unavailable"),
        successful_query,
    ]

    main()

    assert mock_build_stream.call_count == 2

    spark.streams.awaitAnyTermination.assert_called_once_with()

    output = capsys.readouterr().out

    assert (
        "Failed to start stream for topic "
        "'transactional.orders'"
        in output
    )
    
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "BUSINESS_TOPICS",
    (
        "transactional.orders",
        "behavioral.events",
    ),
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "build_stream"
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "build_spark"
)
def test_main_raises_when_no_stream_is_started(
    mock_build_spark,
    mock_build_stream,
):
    spark = MagicMock()

    mock_build_spark.return_value = spark
    mock_build_stream.return_value = None

    with pytest.raises(
        RuntimeError,
        match="No streams were started successfully",
    ):
        main()

    spark.streams.awaitAnyTermination.assert_not_called()

@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "BUSINESS_TOPICS",
    (
        "transactional.orders",
        "behavioral.events",
    ),
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "build_stream"
)
@patch(
    "spark_apps.bronze.jobs.bronze_topic_job."
    "build_spark"
)
def test_main_raises_when_all_topics_fail(
    mock_build_spark,
    mock_build_stream,
):
    spark = MagicMock()

    mock_build_spark.return_value = spark
    mock_build_stream.side_effect = RuntimeError(
        "stream failed"
    )

    with pytest.raises(
        RuntimeError,
        match="No streams were started successfully",
    ):
        main()

    assert mock_build_stream.call_count == 2
    spark.streams.awaitAnyTermination.assert_not_called()