from datetime import datetime
from decimal import Decimal

from pyspark.sql import Row

from spark_apps.silver.facts.fact_order_item import build_fact_order_item_source


def test_fact_order_item_quarantines_field_and_parent_order_failures(spark):
    items_df = spark.createDataFrame(
        [
            Row(
                order_item_id=None,
                order_id=None,
                product_id=None,
                quantity=0,
                unit_price=Decimal("10.00"),
                item_total_amount=Decimal("5.00"),
                kafka_timestamp=datetime(2024, 1, 2, 10, 0),
                kafka_partition=0,
                kafka_offset=1,
            ),
            Row(
                order_item_id="item-2",
                order_id="missing-order",
                product_id="product-1",
                quantity=1,
                unit_price=Decimal("10.00"),
                item_total_amount=Decimal("10.00"),
                kafka_timestamp=datetime(2024, 1, 2, 10, 1),
                kafka_partition=0,
                kafka_offset=2,
            ),
        ]
    )
    fact_order_df = spark.createDataFrame(
        [],
        "order_id string, order_sk long, order_date_sk int, order_timestamp timestamp",
    )
    dim_product_df = spark.createDataFrame(
        [],
        "product_id string, product_sk long, effective_from timestamp, effective_to timestamp",
    )

    valid_df, invalid_df = build_fact_order_item_source(
        items_df,
        fact_order_df,
        dim_product_df,
    )

    assert valid_df.count() == 0

    invalid = {
        row.kafka_offset: row
        for row in invalid_df.collect()
    }

    field_error = invalid[1]
    assert field_error._dq_entity == "order_item"
    assert field_error._dq_source_topic == "transactional.order_items"
    assert "missing_order_item_id" in field_error._dq_error_reason
    assert "missing_order_id" in field_error._dq_error_reason
    assert "missing_product_id" in field_error._dq_error_reason
    assert "non_positive_quantity" in field_error._dq_error_reason
    assert "item_total_mismatch" in field_error._dq_error_reason

    orphan = invalid[2]
    assert orphan._dq_entity == "order_item"
    assert orphan._dq_source_topic == "transactional.order_items"
    assert orphan._dq_error_reason == "missing_parent_order"
