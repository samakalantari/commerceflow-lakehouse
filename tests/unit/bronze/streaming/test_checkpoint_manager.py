import pytest

from spark_apps.bronze.streaming.checkpoint_manager import (
    build_checkpoint_path,
    normalize_name,
)


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("transactional.orders", "transactional_orders"),
        ("Behavioral.Events", "behavioral_events"),
        ("debug-console", "debug-console"),
        ("query_name", "query_name"),
        ("query name", "query_name"),
        ("query/name", "query_name"),
        ("  query name  ", "query_name"),
        ("___query___", "query"),
        ("---query---", "---query---"),
        ("query@v1", "query_v1"),
    ],
)
def test_normalize_name_returns_safe_lowercase_name(
    raw_value,
    expected,
):
    assert normalize_name(raw_value) == expected


def test_normalize_name_replaces_repeated_special_characters_once():
    result = normalize_name(
        "transactional...orders///stream"
    )

    assert result == "transactional_orders_stream"


def test_normalize_name_removes_leading_and_trailing_underscores():
    result = normalize_name(
        "///transactional.orders///"
    )

    assert result == "transactional_orders"


def test_normalize_name_preserves_hyphens_and_underscores():
    result = normalize_name(
        "Debug_Query-v2"
    )

    assert result == "debug_query-v2"


def test_build_checkpoint_path_builds_expected_path():
    result = build_checkpoint_path(
        base_path="/opt/spark-data/checkpoints",
        topic="transactional.orders",
        query_name="debug-console",
        version="v1",
    )

    assert result == (
        "/opt/spark-data/checkpoints/"
        "debug-console/"
        "transactional_orders/"
        "v1"
    )


def test_build_checkpoint_path_supports_s3a_base_path():
    result = build_checkpoint_path(
        base_path=(
            "s3a://commerceflow-lakehouse/"
            "checkpoints/bronze"
        ),
        topic="behavioral.events",
        query_name="bronze-writer",
        version="v2",
    )

    assert result == (
        "s3a://commerceflow-lakehouse/"
        "checkpoints/bronze/"
        "bronze-writer/"
        "behavioral_events/"
        "v2"
    )


def test_build_checkpoint_path_removes_trailing_base_slash():
    result = build_checkpoint_path(
        base_path="/opt/spark-data/checkpoints/",
        topic="transactional.orders",
        query_name="bronze",
        version="v1",
    )

    assert result == (
        "/opt/spark-data/checkpoints/"
        "bronze/"
        "transactional_orders/"
        "v1"
    )


def test_build_checkpoint_path_normalizes_all_path_segments():
    result = build_checkpoint_path(
        base_path="/checkpoints",
        topic="Transactional.Orders",
        query_name="Bronze Writer",
        version="Version 2",
    )

    assert result == (
        "/checkpoints/"
        "bronze_writer/"
        "transactional_orders/"
        "version_2"
    )


def test_build_checkpoint_path_uses_v1_by_default():
    result = build_checkpoint_path(
        base_path="/checkpoints",
        topic="transactional.orders",
        query_name="bronze",
    )

    assert result == (
        "/checkpoints/"
        "bronze/"
        "transactional_orders/"
        "v1"
    )


@pytest.mark.parametrize(
    "invalid_base_path",
    [
        "",
        None,
    ],
)
def test_build_checkpoint_path_rejects_empty_base_path(
    invalid_base_path,
):
    with pytest.raises(
        ValueError,
        match="Checkpoint base path must not be empty",
    ):
        build_checkpoint_path(
            base_path=invalid_base_path,
            topic="transactional.orders",
            query_name="bronze",
        )


@pytest.mark.parametrize(
    "invalid_topic",
    [
        "",
        None,
    ],
)
def test_build_checkpoint_path_rejects_empty_topic(
    invalid_topic,
):
    with pytest.raises(
        ValueError,
        match="Kafka topic must not be empty",
    ):
        build_checkpoint_path(
            base_path="/checkpoints",
            topic=invalid_topic,
            query_name="bronze",
        )


def test_build_checkpoint_path_rejects_empty_query_name():
    with pytest.raises(
        ValueError,
        match="Query name must not be empty",
    ):
        build_checkpoint_path(
            base_path="/checkpoints",
            topic="transactional.orders",
            query_name="",
        )


def test_build_checkpoint_path_rejects_empty_version():
    with pytest.raises(
        ValueError,
        match="Checkpoint version must not be empty",
    ):
        build_checkpoint_path(
            base_path="/checkpoints",
            topic="transactional.orders",
            query_name="bronze",
            version="",
        )