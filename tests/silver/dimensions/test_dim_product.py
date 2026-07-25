from datetime import datetime

from pyspark.sql import Row
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_apps.silver.dimensions.dim_product import (
    build_dim_product_source,
)


def test_build_dim_product_creates_scd2_history(spark):
    products_df = spark.createDataFrame(
        [
            Row(
                product_id="p1",
                name="Laptop",
                price="1000",
                kafka_timestamp=datetime(2024, 1, 1, 10, 0),
                kafka_partition=0,
                kafka_offset=1,
            )
        ]
    )

    price_history_df = spark.createDataFrame(
        [
            Row(
                product_id="p1",
                price="800",
                valid_from=datetime(2023, 12, 1),
                kafka_timestamp=datetime(2023, 12, 1, 10, 0),
            )
        ]
    )

    valid_df, invalid_df = build_dim_product_source(
        products_df,
        price_history_df,
    )

    rows = (
        valid_df
        .filter("product_id = 'p1'")
        .orderBy("effective_from")
        .collect()
    )

    assert len(rows) == 2

    assert rows[0].price == 800
    assert rows[0].is_current is False
    assert rows[0].effective_to is not None

    assert rows[1].price == 1000
    assert rows[1].is_current is True
    assert rows[1].effective_to is None

    assert invalid_df.count() == 0


def test_build_dim_product_quarantines_invalid_product_snapshot(spark):
    products_schema = StructType(
        [
            StructField("product_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("price", StringType(), True),
            StructField("kafka_timestamp", TimestampType(), True),
            StructField("kafka_partition", IntegerType(), True),
            StructField("kafka_offset", IntegerType(), True),
        ]
    )

    products_df = spark.createDataFrame(
        [
            (
                None,
                "Invalid Product",
                "100",
                datetime(2024, 1, 1),
                0,
                1,
            )
        ],
        schema=products_schema,
    )

    price_history_df = spark.createDataFrame(
        [],
        """
        product_id string,
        price string,
        valid_from timestamp,
        kafka_timestamp timestamp
        """,
    )

    valid_df, invalid_df = build_dim_product_source(
        products_df,
        price_history_df,
    )

    assert valid_df.count() == 0

    invalid_row = invalid_df.collect()[0]

    assert invalid_row._dq_entity == "product_snapshot"
    assert "missing_product_id" in invalid_row._dq_error_reason


def test_build_dim_product_casts_price_safely(spark):
    products_df = spark.createDataFrame(
        [
            Row(
                product_id="p1",
                name="Phone",
                price="250.50",
                kafka_timestamp=datetime(2024, 1, 1),
                kafka_partition=0,
                kafka_offset=1,
            )
        ]
    )

    price_history_df = spark.createDataFrame(
        [],
        """
        product_id string,
        price string,
        valid_from timestamp,
        kafka_timestamp timestamp
        """,
    )

    valid_df, invalid_df = build_dim_product_source(
        products_df,
        price_history_df,
    )

    row = valid_df.collect()[0]

    assert row.price == 250.50
    assert invalid_df.count() == 0