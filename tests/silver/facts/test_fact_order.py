from datetime import datetime
from decimal import Decimal

from pyspark.sql import Row

from spark_apps.silver.facts.fact_order import build_fact_order_source


def test_fact_order_preserves_unknown_user_and_quarantines_invalid_order(spark):
    orders_df = spark.createDataFrame(
        [
            Row(
                order_id=" order-1 ",
                user_id="late-user",
                timestamp=datetime(2024, 1, 2, 10, 0),
                total=Decimal("25.00"),
                status=" CREATED ",
                payment_method=" CREDIT_CARD ",
                kafka_timestamp=datetime(2024, 1, 2, 10, 1),
                kafka_partition=0,
                kafka_offset=1,
            ),
            Row(
                order_id=None,
                user_id=None,
                timestamp=None,
                total=Decimal("-1.00"),
                status=" ",
                payment_method=" ",
                kafka_timestamp=datetime(2024, 1, 2, 10, 2),
                kafka_partition=0,
                kafka_offset=2,
            ),
        ]
    )
    dim_user_df = spark.createDataFrame([], "user_id string, user_sk long")

    valid_df, invalid_df = build_fact_order_source(orders_df, dim_user_df)

    valid = valid_df.first()
    invalid = invalid_df.first()

    assert valid.order_id == "order-1"
    assert valid.user_sk == -1
    assert valid.status == "created"
    assert valid.payment_method == "credit_card"

    assert invalid._dq_entity == "order"
    assert invalid._dq_source_topic == "transactional.orders"
    assert "missing_order_id" in invalid._dq_error_reason
    assert "missing_user_id" in invalid._dq_error_reason
    assert "missing_order_timestamp" in invalid._dq_error_reason
    assert "negative_order_total" in invalid._dq_error_reason
    assert "missing_order_status" in invalid._dq_error_reason
    assert "missing_payment_method" in invalid._dq_error_reason
