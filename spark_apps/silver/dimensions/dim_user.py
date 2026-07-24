from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

VALID_LOYALTY_TIERS = (
    "Bronze",
    "Silver",
    "Gold",
    "Platinum",
)


def build_dim_user_source(
    bronze_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Clean and validate Bronze user records.

    Returns:
        valid_df
        invalid_df
    """

    # ---------------------------------------------------------
    # 1. Basic cleansing / normalization
    # ---------------------------------------------------------

    df = (
        bronze_df.withColumn(
            "user_id",
            F.trim(F.col("user_id")),
        )
        .withColumn(
            "username",
            F.trim(F.col("username")),
        )
        .withColumn(
            "email",
            F.lower(F.trim(F.col("email"))),
        )
        .withColumn(
            "device",
            F.when(
                F.col("device").isNull() | (F.length(F.trim(F.col("device"))) == 0),
                F.lit("Unknown"),
            ).otherwise(
                F.trim(F.col("device")),
            ),
        )
        .withColumn(
            "loyalty_tier",
            F.initcap(F.lower(F.trim(F.col("loyalty_tier")))),
        )
        .withColumn(
            "location",
            F.trim(F.col("location")),
        )
    )

    # ---------------------------------------------------------
    # 2. Deduplicate valid business keys
    # ---------------------------------------------------------

    identified_users = df.filter(
        F.col("user_id").isNotNull() & (F.length(F.trim(F.col("user_id"))) > 0)
    )

    unidentified_users = df.filter(
        F.col("user_id").isNull() | (F.length(F.trim(F.col("user_id"))) == 0)
    )

    window = Window.partitionBy("user_id").orderBy(
        F.col("kafka_timestamp").desc_nulls_last(),
        F.col("kafka_partition").desc_nulls_last(),
        F.col("kafka_offset").desc_nulls_last(),
    )

    deduplicated_users = (
        identified_users.withColumn(
            "_row_number",
            F.row_number().over(window),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    df = deduplicated_users.unionByName(unidentified_users)

    # ---------------------------------------------------------
    # 3. Data quality rules
    # ---------------------------------------------------------

    email_pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

    df = df.withColumn(
        "_dq_error_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("user_id").isNull() | (F.length(F.col("user_id")) == 0),
                F.lit("missing_user_id"),
            ),
            F.when(
                F.col("username").isNull() | (F.length(F.col("username")) == 0),
                F.lit("missing_username"),
            ),
            F.when(
                F.col("email").isNull() | (F.length(F.col("email")) == 0),
                F.lit("missing_email"),
            ),
            F.when(
                F.col("email").isNotNull()
                & (F.length(F.col("email")) > 0)
                & (F.length(F.col("email")) > 0)
                & ~F.col("email").rlike(email_pattern),
                F.lit("invalid_email"),
            ),
            F.when(
                F.col("signup_date").isNull(),
                F.lit("missing_signup_date"),
            ),
            F.when(
                F.col("signup_date") > F.current_date(),
                F.lit("future_signup_date"),
            ),
            F.when(
                F.col("loyalty_tier").isNull() | ~F.col("loyalty_tier").isin(*VALID_LOYALTY_TIERS),
                F.lit("invalid_loyalty_tier"),
            ),
            F.when(
                F.col("location").isNull() | (F.length(F.col("location")) == 0),
                F.lit("missing_location"),
            ),
            F.when(
                F.col("kafka_timestamp").isNull(),
                F.lit("missing_kafka_timestamp"),
            ),
        ),
    )

    # ---------------------------------------------------------
    # 4. Split valid / invalid
    # ---------------------------------------------------------

    valid_df = df.filter(F.col("_dq_error_reason") == "").select(
        # Deterministic surrogate key
        F.xxhash64("user_id").alias("user_sk"),
        "user_id",
        "username",
        "email",
        "signup_date",
        "device",
        "loyalty_tier",
        "location",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("username"),
                F.col("email"),
                F.col("signup_date").cast("string"),
                F.col("device"),
                F.col("loyalty_tier"),
                F.col("location"),
            ),
            256,
        ).alias("record_hash"),
        F.col("kafka_timestamp").alias("source_kafka_timestamp"),
    )

    invalid_df = (
        df.filter(F.col("_dq_error_reason") != "")
        .withColumn(
            "_dq_quarantine_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit("transactional.users"),
                    F.coalesce(
                        F.col("kafka_partition").cast("string"),
                        F.lit("unknown_partition"),
                    ),
                    F.coalesce(
                        F.col("kafka_offset").cast("string"),
                        F.lit("unknown_offset"),
                    ),
                    F.coalesce(
                        F.col("kafka_timestamp").cast("string"),
                        F.lit("unknown_timestamp"),
                    ),
                    F.coalesce(
                        F.col("user_id"),
                        F.lit("unknown_user"),
                    ),
                ),
                256,
            ),
        )
        .withColumn(
            "_dq_entity",
            F.lit("user"),
        )
        .withColumn(
            "_dq_source_topic",
            F.lit("transactional.users"),
        )
        .withColumn(
            "_dq_status",
            F.lit("open"),
        )
        .withColumn(
            "_dq_quarantined_at",
            F.current_timestamp(),
        )
    )
    return (
        valid_df,
        invalid_df,
    )
