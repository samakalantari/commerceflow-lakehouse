import pytest
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from spark_apps.bronze.transforms.behavioral_transform import (
    group_behavioral_event_fields,
)


def test_groups_behavioral_specific_fields_into_event_data(
    spark,
):
    schema = StructType(
        [
            StructField(
                "kafka_key",
                StringType(),
                True,
            ),
            StructField(
                "kafka_topic",
                StringType(),
                True,
            ),
            StructField(
                "timestamp",
                StringType(),
                False,
            ),
            StructField(
                "user_id",
                StringType(),
                False,
            ),
            StructField(
                "event_type",
                StringType(),
                False,
            ),
            StructField(
                "device",
                StringType(),
                False,
            ),
            StructField(
                "session_id",
                StringType(),
                False,
            ),
            StructField(
                "product_id",
                StringType(),
                True,
            ),
            StructField(
                "quantity",
                IntegerType(),
                True,
            ),
            StructField(
                "success",
                BooleanType(),
                True,
            ),
            StructField(
                "url_path",
                StringType(),
                True,
            ),
        ]
    )

    df = spark.createDataFrame(
        [
            (
                "U1",
                "behavioral.events",
                "2026-07-16T10:00:00",
                "U1",
                "add_to_cart",
                "mobile",
                "session-1",
                "P1",
                2,
                None,
                None,
            )
        ],
        schema=schema,
    )

    result = group_behavioral_event_fields(
        df,
        "behavioral.events",
    )

    assert result.columns == [
        "kafka_key",
        "kafka_topic",
        "timestamp",
        "user_id",
        "event_type",
        "device",
        "session_id",
        "event_data",
    ]

    event_data_type = (
        result.schema["event_data"].dataType
    )

    assert event_data_type.fieldNames() == [
        "product_id",
        "quantity",
        "url_path",
        "success",
    ]

    row = result.first()

    assert row.event_data.product_id == "P1"
    assert row.event_data.quantity == 2
    assert row.event_data.success is None
    assert row.event_data.url_path is None


def test_returns_non_behavioral_topic_unchanged(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "O1",
                "completed",
            )
        ],
        [
            "order_id",
            "status",
        ],
    )

    result = group_behavioral_event_fields(
        df,
        "transactional.orders",
    )

    assert result is df


def test_raises_when_common_behavioral_column_is_missing(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "U1",
                "page_view",
            )
        ],
        [
            "user_id",
            "event_type",
        ],
    )

    with pytest.raises(
        ValueError,
        match="Missing required behavioral event columns",
    ):
        group_behavioral_event_fields(
            df,
            "behavioral.events",
        )