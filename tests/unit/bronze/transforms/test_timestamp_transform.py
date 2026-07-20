from datetime import date, datetime

import pytest
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_apps.bronze.transforms.timestamp_transform import (
    add_time_partitions,
)


def assert_partition_date(
    row,
    *,
    year: int,
    month: int,
    day: int,
):
    assert row["year"] == year
    assert row["month"] == month
    assert row["day"] == day


def test_add_time_partitions_uses_configured_field_instead_of_ingested_at(
    spark,
):
    schema = StructType(
        [
            StructField(
                "order_id",
                StringType(),
                False,
            ),
            StructField(
                "timestamp",
                StringType(),
                True,
            ),
            StructField(
                "ingested_at",
                StringType(),
                True,
            ),
        ]
    )

    df = spark.createDataFrame(
        [
            (
                "order-1",
                "2026-07-15 18:30:00",
                "2026-07-16 01:00:00",
            )
        ],
        schema=schema,
    )

    row = add_time_partitions(
        df,
        "transactional.orders",
    ).first()

    assert_partition_date(
        row,
        year=2026,
        month=7,
        day=15,
    )


@pytest.mark.parametrize(
    "timestamp_value",
    [
        "2026-07-15 18:30:00",
        "2026-07-15 18:30:00.123",
    ],
    ids=[
        "space-separated-seconds",
        "space-separated-milliseconds",
    ],
)
def test_add_time_partitions_accepts_standard_datetime_strings(
    spark,
    timestamp_value,
):
    df = spark.createDataFrame(
        [
            {
                "timestamp": timestamp_value,
            }
        ]
    )

    row = add_time_partitions(
        df,
        "transactional.orders",
    ).first()

    assert_partition_date(
        row,
        year=2026,
        month=7,
        day=15,
    )


@pytest.mark.parametrize(
    "timestamp_value",
    [
        "2026-07-15T18:30:00",
        "2026-07-15T18:30:00.123",
    ],
    ids=[
        "iso-local-seconds",
        "iso-local-milliseconds",
    ],
)
def test_add_time_partitions_accepts_iso_local_datetime_strings(
    spark,
    timestamp_value,
):
    df = spark.createDataFrame(
        [
            {
                "timestamp": timestamp_value,
            }
        ]
    )

    row = add_time_partitions(
        df,
        "transactional.orders",
    ).first()

    assert_partition_date(
        row,
        year=2026,
        month=7,
        day=15,
    )


def test_add_time_partitions_accepts_timestamp_type(
    spark,
):
    schema = StructType(
        [
            StructField(
                "timestamp",
                TimestampType(),
                True,
            ),
        ]
    )

    df = spark.createDataFrame(
        [
            (
                datetime(
                    2026,
                    7,
                    15,
                    18,
                    30,
                    45,
                ),
            ),
        ],
        schema=schema,
    )

    row = add_time_partitions(
        df,
        "transactional.orders",
    ).first()

    assert_partition_date(
        row,
        year=2026,
        month=7,
        day=15,
    )


@pytest.mark.parametrize(
    "timestamp_value",
    [
        "2026-07-15T18:30:00",
        "2026-07-15T18:30:00.123",
    ],
    ids=[
        "iso-local-seconds",
        "iso-local-milliseconds",
    ],
)
def test_add_time_partitions_accepts_iso_local_datetime_strings(
    spark,
    timestamp_value,
):
    df = spark.createDataFrame(
        [
            {
                "timestamp": timestamp_value,
            }
        ]
    )

    row = add_time_partitions(
        df,
        "transactional.orders",
    ).first()

    assert_partition_date(
        row,
        year=2026,
        month=7,
        day=15,
    )


def test_add_time_partitions_normalizes_offset_before_partitioning(
    spark,
):
    # Local time: 2026-07-16 01:30:00 at UTC+03:30
    # UTC time:   2026-07-15 22:00:00
    # Therefore, the UTC partition date must be July 15.
    df = spark.createDataFrame(
        [
            {
                "timestamp": (
                    "2026-07-16T01:30:00+03:30"
                ),
            }
        ]
    )

    row = add_time_partitions(
        df,
        "behavioral.events",
    ).first()

    assert_partition_date(
        row,
        year=2026,
        month=7,
        day=15,
    )


@pytest.mark.parametrize(
    "ambiguous_value",
    [
        "07/08/2026",
        "08/07/2026",
        "07-08-2026",
        "15/07/2026 18:30:00",
    ],
    ids=[
        "slash-month-day-or-day-month",
        "reverse-slash-month-day-or-day-month",
        "dash-day-month-or-month-day",
        "localized-datetime",
    ],
)
def test_add_time_partitions_does_not_accept_ambiguous_strings(
    spark,
    ambiguous_value,
):
    df = spark.createDataFrame(
        [
            {
                "timestamp": ambiguous_value,
            }
        ]
    )

    row = add_time_partitions(
        df,
        "transactional.orders",
    ).first()

    assert row["year"] is None
    assert row["month"] is None
    assert row["day"] is None


def test_add_time_partitions_accepts_epoch_seconds(spark):
    schema = StructType(
        [
            StructField(
                "timestamp",
                LongType(),
                True,
            ),
        ]
    )

    df = spark.createDataFrame(
        [
            (1784138400,),
        ],
        schema=schema,
    )

    row = add_time_partitions(
        df,
        "behavioral.events",
    ).first()

    assert_partition_date(
        row,
        year=2026,
        month=7,
        day=15,
    )


def test_add_time_partitions_accepts_epoch_milliseconds(spark):
    schema = StructType(
        [
            StructField(
                "timestamp",
                LongType(),
                True,
            ),
        ]
    )

    df = spark.createDataFrame(
        [
            (1784138400000,),
        ],
        schema=schema,
    )

    row = add_time_partitions(
        df,
        "behavioral.events",
    ).first()

    assert_partition_date(
        row,
        year=2026,
        month=7,
        day=15,
    )


def test_add_time_partitions_falls_back_to_ingested_at(
    spark,
    capsys,
):
    df = spark.createDataFrame(
        [
            {
                "order_id": "order-1",
                "ingested_at": "2026-09-11 14:20:00",
            }
        ]
    )

    row = add_time_partitions(
        df,
        "transactional.orders",
    ).first()

    output = capsys.readouterr().out

    assert "Falling back to 'ingested_at'" in output

    assert_partition_date(
        row,
        year=2026,
        month=9,
        day=11,
    )


def test_add_time_partitions_raises_when_configured_and_fallback_fields_missing(
    spark,
):
    df = spark.createDataFrame(
        [
            {
                "order_id": "order-1",
                "status": "paid",
            }
        ]
    )

    with pytest.raises(
        ValueError,
        match="timestamp",
    ):
        add_time_partitions(
            df,
            "transactional.orders",
        )


def test_add_time_partitions_rejects_unsupported_topic(
    spark,
):
    df = spark.createDataFrame(
        [
            {
                "ingested_at": (
                    "2026-07-15 18:30:00"
                ),
            }
        ]
    )

    with pytest.raises(
        ValueError,
        match="No partition configuration found",
    ):
        add_time_partitions(
            df,
            "transactional.unknown",
        )


def test_add_time_partitions_preserves_original_columns(
    spark,
):
    df = spark.createDataFrame(
        [
            {
                "order_id": "order-1",
                "timestamp": (
                    "2026-07-15 18:30:00"
                ),
                "status": "paid",
            }
        ]
    )

    result_df = add_time_partitions(
        df,
        "transactional.orders",
    )

    assert result_df.columns == [
        "order_id",
        "status",
        "timestamp",
        "year",
        "month",
        "day",
    ]


def test_add_time_partitions_removes_internal_helper_column(
    spark,
):
    df = spark.createDataFrame(
        [
            {
                "timestamp": (
                    "2026-07-15 18:30:00"
                ),
            }
        ]
    )

    result_df = add_time_partitions(
        df,
        "transactional.orders",
    )

    assert (
        "__partition_timestamp"
        not in result_df.columns
    )


def test_add_time_partitions_returns_null_for_invalid_timestamp(
    spark,
):
    df = spark.createDataFrame(
        [
            {
                "timestamp": (
                    "not-a-valid-timestamp"
                ),
            }
        ]
    )

    row = add_time_partitions(
        df,
        "transactional.orders",
    ).first()

    assert row["year"] is None
    assert row["month"] is None
    assert row["day"] is None


def test_add_time_partitions_returns_null_for_null_timestamp(
    spark,
):
    schema = StructType(
        [
            StructField(
                "timestamp",
                StringType(),
                True,
            ),
        ]
    )

    df = spark.createDataFrame(
        [
            (None,),
        ],
        schema=schema,
    )

    row = add_time_partitions(
        df,
        "transactional.orders",
    ).first()

    assert row["year"] is None
    assert row["month"] is None
    assert row["day"] is None

    
def test_add_time_partitions_returns_categories_unchanged(
    spark,
):
    schema = StructType(
        [
            StructField(
                "category_id",
                StringType(),
                False,
            ),
            StructField(
                "name",
                StringType(),
                True,
            ),
            StructField(
                "parent_category_id",
                StringType(),
                True,
            ),
            StructField(
                "ingested_at",
                StringType(),
                False,
            ),
        ]
    )

    df = spark.createDataFrame(
        [
            (
                "C1",
                "Electronics",
                None,
                "2026-07-16 10:00:00",
            )
        ],
        schema=schema,
    )

    result_df = add_time_partitions(
        df=df,
        topic="transactional.categories",
    )

    assert result_df.columns == df.columns

    assert "year" not in result_df.columns
    assert "month" not in result_df.columns
    assert "day" not in result_df.columns

    assert (
        result_df.first().asDict()
        == df.first().asDict()
    )