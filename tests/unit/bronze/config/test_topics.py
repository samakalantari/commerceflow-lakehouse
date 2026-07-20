import pytest

from spark_apps.bronze.config.topic_metadata import (
    get_partition_columns,
    get_partition_config,
    get_partition_timestamp_field,
)


def test_categories_are_not_partitioned():
    assert (
        get_partition_config(
            "transactional.categories"
        )
        is None
    )

    assert get_partition_columns(
        "transactional.categories"
    ) == ()

    assert get_partition_timestamp_field(
        "transactional.categories"
    ) is None


def test_orders_use_timestamp_partitions():
    config = get_partition_config(
        "transactional.orders"
    )

    assert config is not None

    assert (
        config["timestamp_field"]
        == "timestamp"
    )

    assert config["columns"] == (
        "year",
        "month",
        "day",
    )


def test_unknown_topic_raises():
    with pytest.raises(
        ValueError,
        match="No partition configuration",
    ):
        get_partition_config(
            "transactional.unknown"
        )