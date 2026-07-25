from datetime import datetime
from decimal import Decimal

from pyspark.sql import Row

from spark_apps.silver.facts.fact_return_refund import build_fact_return_refund_source


def test_return_refund_validates_source_and_relationships(spark):
    returns_df = spark.createDataFrame(
        [
            Row(
                return_refund_id=" RR1 ",
                order_id=" O1 ",
                order_item_id=" OI1 ",
                return_timestamp=datetime(2024, 1, 3),
                refund_amount=Decimal("10.00"),
                return_reason=" DAMAGED ",
                kafka_timestamp=datetime(2024, 1, 3, 0, 1),
                kafka_partition=0,
                kafka_offset=1,
            ),
            Row(
                return_refund_id=None,
                order_id=None,
                order_item_id=None,
                return_timestamp=None,
                refund_amount=Decimal("-1.00"),
                return_reason=" ",
                kafka_timestamp=datetime(2024, 1, 3, 0, 2),
                kafka_partition=0,
                kafka_offset=2,
            ),
            Row(
                return_refund_id="RR3",
                order_id="wrong-order",
                order_item_id="OI1",
                return_timestamp=datetime(2023, 12, 31),
                refund_amount=Decimal("5.00"),
                return_reason="wrong_item",
                kafka_timestamp=datetime(2024, 1, 3, 0, 3),
                kafka_partition=0,
                kafka_offset=3,
            ),
        ]
    )
    fact_order_item_df = spark.createDataFrame(
        [
            Row(
                order_item_id="OI1",
                order_item_sk=101,
                order_id="O1",
                order_sk=11,
                order_timestamp=datetime(2024, 1, 1),
            )
        ]
    )

    valid_df, invalid_df = build_fact_return_refund_source(
        returns_df,
        fact_order_item_df,
    )

    valid = valid_df.first()
    assert valid.return_refund_id == "RR1"
    assert valid.order_sk == 11
    assert valid.order_item_sk == 101
    assert valid.refund_amount == Decimal("10.00")
    assert valid.return_reason == "damaged"
    assert valid.return_date_sk == 20240103

    invalid = {row.kafka_offset: row for row in invalid_df.collect()}

    source_error = invalid[2]
    assert source_error._dq_entity == "return_refund"
    assert source_error._dq_source_topic == "transactional.returns_refunds"
    assert "missing_return_refund_id" in source_error._dq_error_reason
    assert "missing_order_id" in source_error._dq_error_reason
    assert "missing_order_item_id" in source_error._dq_error_reason
    assert "missing_return_timestamp" in source_error._dq_error_reason
    assert "negative_refund_amount" in source_error._dq_error_reason
    assert "missing_return_reason" in source_error._dq_error_reason

    relationship_error = invalid[3]
    assert "order_item_order_mismatch" in relationship_error._dq_error_reason
    assert "return_before_order" in relationship_error._dq_error_reason
