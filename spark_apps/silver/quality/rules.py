from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_null_error_reason(
    df: DataFrame,
    required_columns: Iterable[str],
) -> DataFrame:
    """
    Add a data quality error reason when any required
    business column is null.
    """
    required_columns = list(required_columns)

    if not required_columns:
        return df.withColumn(
            "_dq_error_reason",
            F.lit(None).cast("string"),
        )

    conditions = [
        F.col(column).isNull()
        for column in required_columns
    ]

    null_condition = conditions[0]

    for condition in conditions[1:]:
        null_condition = (
            null_condition | condition
        )

    return df.withColumn(
        "_dq_error_reason",
        F.when(
            null_condition,
            F.lit(
                "required_field_is_null"
            ),
        ),
    )


def split_valid_invalid(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Split records into valid and invalid datasets
    using the internal data quality error column.
    """
    valid_df = (
        df
        .filter(
            F.col(
                "_dq_error_reason"
            ).isNull()
        )
        .drop(
            "_dq_error_reason"
        )
    )

    invalid_df = (
        df
        .filter(
            F.col(
                "_dq_error_reason"
            ).isNotNull()
        )
    )

    return valid_df, invalid_df
