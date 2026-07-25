from unittest.mock import MagicMock, call

import pytest

from spark_apps.silver.common.bronze_reader import (
    bronze_topic_path,
    bronze_topic_paths,
    read_bronze_topic,
)


BRONZE_BASE = "s3a://commerceflow-lakehouse/bronze"


@pytest.mark.parametrize(
    ("topic", "suffix"),
    [
        ("transactional.orders", "transactional/orders_recovery"),
        (
            "transactional.product_price_history",
            "transactional/product_price_history_recovery",
        ),
        ("transactional.users", "transactional/users_recovery"),
        ("transactional.returns_refunds", "transactional/returns_refunds"),
    ],
)
def test_bronze_topic_path_uses_active_output(monkeypatch, topic, suffix):
    monkeypatch.setenv("BRONZE_KAFKA_BASE_PATH", f"{BRONZE_BASE}/")

    assert bronze_topic_path(topic) == f"{BRONZE_BASE}/{suffix}"


def test_recovery_topic_paths_include_original_and_recovery(monkeypatch):
    monkeypatch.setenv("BRONZE_KAFKA_BASE_PATH", BRONZE_BASE)

    assert bronze_topic_paths("transactional.orders") == (
        f"{BRONZE_BASE}/transactional/orders",
        f"{BRONZE_BASE}/transactional/orders_recovery",
    )


def test_regular_topic_paths_include_only_original(monkeypatch):
    monkeypatch.setenv("BRONZE_KAFKA_BASE_PATH", BRONZE_BASE)

    assert bronze_topic_paths("transactional.returns_refunds") == (
        f"{BRONZE_BASE}/transactional/returns_refunds",
    )


def test_read_recovery_topic_unions_original_and_recovery(monkeypatch):
    monkeypatch.setenv("BRONZE_KAFKA_BASE_PATH", BRONZE_BASE)

    spark = MagicMock()
    reader = spark.read
    original_df = MagicMock()
    recovery_df = MagicMock()
    combined_df = MagicMock()

    reader.option.return_value = reader
    reader.parquet.side_effect = [original_df, recovery_df]
    original_df.unionByName.return_value = combined_df

    result = read_bronze_topic(spark, "transactional.orders")

    assert result is combined_df
    assert reader.option.call_args_list == [
        call("basePath", f"{BRONZE_BASE}/transactional/orders"),
        call("basePath", f"{BRONZE_BASE}/transactional/orders_recovery"),
    ]
    assert reader.parquet.call_args_list == [
        call(f"{BRONZE_BASE}/transactional/orders/year=*/month=*/day=*"),
        call(f"{BRONZE_BASE}/transactional/orders_recovery/year=*/month=*/day=*"),
    ]
    original_df.unionByName.assert_called_once_with(
        recovery_df,
        allowMissingColumns=True,
    )
