from datetime import date, datetime

from pyspark.sql import Row

from spark_apps.silver.dimensions.dim_user import build_dim_user_source


def test_build_dim_user_normalizes_valid_record_and_quarantines_invalid_record(spark):
    bronze_df = spark.createDataFrame(
        [
            Row(
                user_id=" user-1 ",
                username=" Alice ",
                email=" ALICE@EXAMPLE.COM ",
                signup_date=date(2024, 1, 1),
                device=None,
                loyalty_tier=" gold ",
                location=" Tehran ",
                kafka_timestamp=datetime(2024, 1, 1, 12, 0),
                kafka_partition=0,
                kafka_offset=1,
            ),
            Row(
                user_id=None,
                username=" ",
                email="invalid-email",
                signup_date=None,
                device="mobile",
                loyalty_tier="unknown",
                location=" ",
                kafka_timestamp=datetime(2024, 1, 1, 12, 1),
                kafka_partition=0,
                kafka_offset=2,
            ),
        ]
    )

    valid_df, invalid_df = build_dim_user_source(bronze_df)

    valid = valid_df.first()
    invalid = invalid_df.first()

    assert valid.user_id == "user-1"
    assert valid.username == "Alice"
    assert valid.email == "alice@example.com"
    assert valid.device == "Unknown"
    assert valid.loyalty_tier == "Gold"
    assert valid.location == "Tehran"

    assert invalid._dq_entity == "user"
    assert invalid._dq_source_topic == "transactional.users"
    assert "missing_user_id" in invalid._dq_error_reason
    assert "missing_username" in invalid._dq_error_reason
    assert "invalid_email" in invalid._dq_error_reason
    assert "missing_signup_date" in invalid._dq_error_reason
    assert "invalid_loyalty_tier" in invalid._dq_error_reason
    assert "missing_location" in invalid._dq_error_reason
