from datetime import date

import pytest

from spark_apps.silver.dimensions.dim_date import build_dim_date


def test_build_dim_date_creates_expected_rows(spark):
    result = build_dim_date(
        spark,
        start_date="2024-01-05",
        end_date="2024-01-07",
    )

    rows = result.orderBy("full_date").collect()

    assert len(rows) == 3

    assert rows[0].date_sk == 20240105
    assert rows[0].full_date == date(2024, 1, 5)
    assert rows[0].year == 2024
    assert rows[0].quarter == 1
    assert rows[0].month == 1
    assert rows[0].day == 5
    assert rows[0].is_weekend is False

    assert rows[1].full_date == date(2024, 1, 6)
    assert rows[1].is_weekend is True

    assert rows[2].full_date == date(2024, 1, 7)
    assert rows[2].is_weekend is True


def test_build_dim_date_rejects_invalid_date_range(spark):
    with pytest.raises(
        ValueError,
        match="start_date must be before or equal to end_date",
    ):
        build_dim_date(
            spark,
            start_date="2024-01-10",
            end_date="2024-01-01",
        )