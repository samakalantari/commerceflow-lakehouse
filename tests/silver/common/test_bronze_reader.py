from datetime import date
from unittest.mock import MagicMock

import pytest

from spark_apps.silver.common.bronze_reader import (
    bronze_topic_day_path,
    bronze_topic_path,
    bronze_topic_paths,
    split_tombstones,
    parse_ingested_date,
    read_bronze_topic,
)


BRONZE_BASE = "s3a://commerceflow-lakehouse/bronze_v2"


@pytest.mark.parametrize("topic", ["transactional.orders", "transactional.users", "transactional.returns_refunds"])
def test_bronze_topic_path_uses_new_data(monkeypatch, topic):
    monkeypatch.setenv("BRONZE_KAFKA_BASE_PATH", f"{BRONZE_BASE}/")
    assert bronze_topic_path(topic) == f"{BRONZE_BASE}/{topic.replace(chr(46), chr(47))}/new_data"


def test_topic_paths_include_historical_and_active_output(monkeypatch):
    monkeypatch.setenv("BRONZE_KAFKA_BASE_PATH", BRONZE_BASE)
    assert bronze_topic_paths("transactional.orders") == (
        f"{BRONZE_BASE}/transactional/orders/historical_v1",
        f"{BRONZE_BASE}/transactional/orders/new_data",
    )


def test_day_path_uses_raw_date_directories(monkeypatch):
    monkeypatch.setenv("BRONZE_KAFKA_BASE_PATH", BRONZE_BASE)
    assert bronze_topic_day_path("transactional.orders", date(2026, 7, 30)) == (
        f"{BRONZE_BASE}/transactional/orders/new_data/2026/07/30"
    )


def test_parse_ingested_date():
    assert parse_ingested_date("2026-07-30") == date(2026, 7, 30)


def test_split_tombstones_removes_null_payload_records(spark):
    df = spark.createDataFrame(
        [("O1", None, None), ("O2", "O2", "user-2")],
        "kafka_key string, order_id string, user_id string",
    )

    records, tombstones = split_tombstones(
        df, business_key="order_id", payload_columns=("order_id", "user_id")
    )

    assert records.count() == 1
    assert tombstones.collect()[0].order_id == "O1"


def test_read_topic_historical_reads_only_historical_directory(monkeypatch):
    monkeypatch.setenv("BRONZE_KAFKA_BASE_PATH", BRONZE_BASE)
    spark = MagicMock()
    reader = spark.read
    historical_df = MagicMock()
    reader.option.return_value = reader
    reader.parquet.return_value = historical_df

    result = read_bronze_topic(spark, "transactional.orders", source_mode="historical")

    assert result is historical_df
    reader.parquet.assert_called_once_with(
        f"{BRONZE_BASE}/transactional/orders/historical_v1"
    )


def test_read_topic_day_reads_only_requested_directory(monkeypatch):
    monkeypatch.setenv("BRONZE_KAFKA_BASE_PATH", BRONZE_BASE)
    spark = MagicMock()
    reader = spark.read
    day_df = MagicMock()
    reader.option.return_value = reader
    reader.parquet.return_value = day_df

    result = read_bronze_topic(
        spark, "transactional.orders", ingested_date=date(2026, 7, 30)
    )

    assert result is day_df
    reader.option.assert_called_once_with("recursiveFileLookup", "true")
    reader.parquet.assert_called_once_with(
        f"{BRONZE_BASE}/transactional/orders/new_data/2026/07/30"
    )
