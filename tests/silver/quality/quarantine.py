from unittest.mock import MagicMock

from pyspark.sql import functions as F

from spark_apps.silver.quality.quarantine import (
    prepare_quarantine_records,
    write_quarantine,
)


def test_prepare_quarantine_records_adds_standard_metadata(
    spark,
) -> None:
    source_df = spark.createDataFrame(
        [
            {
                "user_id": "user-1",
                "kafka_partition": 2,
                "kafka_offset": 15,
                "_dq_error_reason": "invalid_email",
            }
        ]
    )

    result_df = prepare_quarantine_records(
        df=source_df,
        entity_name="user",
        source_topic="transactional.users",
    )

    row = (
        result_df.select(
            "user_id",
            "_dq_quarantine_id",
            "_dq_entity",
            "_dq_source_topic",
            "_dq_error_reason",
            "_dq_status",
            "_dq_quarantined_at",
        )
        .first()
    )

    assert row.user_id == "user-1"
    assert row._dq_quarantine_id is not None
    assert len(row._dq_quarantine_id) == 64
    assert row._dq_entity == "user"
    assert row._dq_source_topic == "transactional.users"
    assert row._dq_error_reason == "invalid_email"
    assert row._dq_status == "open"
    assert row._dq_quarantined_at is not None


def test_prepare_quarantine_records_builds_stable_id(
    spark,
) -> None:
    source_df = spark.createDataFrame(
        [
            {
                "kafka_partition": 1,
                "kafka_offset": 100,
                "_dq_error_reason": "missing_user_id",
            },
            {
                "kafka_partition": 1,
                "kafka_offset": 100,
                "_dq_error_reason": "invalid_email",
            },
        ]
    )

    result_df = prepare_quarantine_records(
        df=source_df,
        entity_name="user",
        source_topic="transactional.users",
    )

    quarantine_ids = [
        row["_dq_quarantine_id"]
        for row in result_df.select(
            "_dq_quarantine_id"
        ).collect()
    ]

    assert len(set(quarantine_ids)) == 1


def test_prepare_quarantine_records_creates_different_ids_for_messages(
    spark,
) -> None:
    source_df = spark.createDataFrame(
        [
            {
                "kafka_partition": 1,
                "kafka_offset": 100,
                "_dq_error_reason": "missing_user_id",
            },
            {
                "kafka_partition": 1,
                "kafka_offset": 101,
                "_dq_error_reason": "missing_user_id",
            },
        ]
    )

    result_df = prepare_quarantine_records(
        df=source_df,
        entity_name="user",
        source_topic="transactional.users",
    )

    quarantine_ids = [
        row["_dq_quarantine_id"]
        for row in result_df.select(
            "_dq_quarantine_id"
        ).collect()
    ]

    assert len(set(quarantine_ids)) == 2


def test_prepare_quarantine_records_handles_missing_kafka_metadata(
    spark,
) -> None:
    source_df = spark.createDataFrame(
        [
            {
                "user_id": "user-1",
                "kafka_partition": None,
                "kafka_offset": None,
                "_dq_error_reason": "missing_kafka_timestamp",
            }
        ],
        schema="""
            user_id STRING,
            kafka_partition INT,
            kafka_offset LONG,
            _dq_error_reason STRING
        """,
    )

    result_df = prepare_quarantine_records(
        df=source_df,
        entity_name="user",
        source_topic="transactional.users",
    )

    row = result_df.select(
        "_dq_quarantine_id",
        "_dq_entity",
        "_dq_source_topic",
    ).first()

    assert row._dq_quarantine_id is not None
    assert len(row._dq_quarantine_id) == 64
    assert row._dq_entity == "user"
    assert row._dq_source_topic == "transactional.users"


def test_write_quarantine_does_nothing_for_empty_dataframe(
    spark,
    monkeypatch,
) -> None:
    empty_df = spark.createDataFrame(
        [],
        schema="""
            _dq_quarantine_id STRING,
            _dq_error_reason STRING
        """,
    )

    write_to_mock = MagicMock()

    monkeypatch.setattr(
        empty_df,
        "writeTo",
        write_to_mock,
    )

    write_quarantine(
        df=empty_df,
        table_name="test_catalog.silver_quarantine.invalid_users",
    )

    write_to_mock.assert_not_called()


def test_write_quarantine_appends_non_empty_dataframe(
    spark,
    monkeypatch,
) -> None:
    df = spark.createDataFrame(
        [
            {
                "_dq_quarantine_id": "quarantine-1",
                "_dq_error_reason": "invalid_email",
            }
        ]
    )

    writer_mock = MagicMock()

    writer_mock.using.return_value = writer_mock
    writer_mock.tableProperty.return_value = writer_mock

    write_to_mock = MagicMock(
        return_value=writer_mock,
    )

    monkeypatch.setattr(
        df,
        "writeTo",
        write_to_mock,
    )

    table_name = (
        "test_catalog."
        "silver_quarantine."
        "invalid_users"
    )

    write_quarantine(
        df=df,
        table_name=table_name,
    )

    write_to_mock.assert_called_once_with(
        table_name
    )
    writer_mock.using.assert_called_once_with(
        "iceberg"
    )
    writer_mock.tableProperty.assert_called_once_with(
        "format-version",
        "2",
    )
    writer_mock.append.assert_called_once_with()
    writer_mock.createOrReplace.assert_not_called()