from pyspark.sql import functions as F

from spark_apps.silver.common.bronze_reader import (
    read_bronze_topic,
)
from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    TOPIC_USERS,
)

BUSINESS_COLUMNS = [
    "user_id",
    "username",
    "email",
    "signup_date",
    "device",
    "loyalty_tier",
    "location",
]


def main() -> None:
    spark = build_iceberg_spark("profile-silver-users")

    try:
        df = read_bronze_topic(
            spark,
            TOPIC_USERS,
        )

        print("=" * 100)
        print("USER DATA PROFILING")
        print("=" * 100)

        total_count = df.count()

        print(f"Total Bronze records: {total_count:,}")

        # -------------------------------------------------
        # Null counts
        # -------------------------------------------------

        print("\nNULL COUNTS")
        print("-" * 100)

        df.select(
            *[
                F.sum(
                    F.when(
                        F.col(column).isNull(),
                        1,
                    ).otherwise(0)
                ).alias(column)
                for column in BUSINESS_COLUMNS
            ]
        ).show(truncate=False)

        # -------------------------------------------------
        # Distinct users
        # -------------------------------------------------

        distinct_users = df.select("user_id").distinct().count()

        print(f"\nDistinct user_id: {distinct_users:,}")

        print(f"Potential duplicate rows: {total_count - distinct_users:,}")

        # -------------------------------------------------
        # Duplicate user versions
        # -------------------------------------------------

        print("\nUSERS WITH MULTIPLE RECORDS")
        print("-" * 100)

        (
            df.groupBy("user_id")
            .count()
            .filter(F.col("count") > 1)
            .orderBy(F.col("count").desc())
            .show(
                20,
                truncate=False,
            )
        )

        # -------------------------------------------------
        # Loyalty tiers
        # -------------------------------------------------

        print("\nLOYALTY TIER VALUES")
        print("-" * 100)

        (df.groupBy("loyalty_tier").count().orderBy(F.col("count").desc()).show(truncate=False))

        # -------------------------------------------------
        # Devices
        # -------------------------------------------------

        print("\nDEVICE VALUES")
        print("-" * 100)

        (df.groupBy("device").count().orderBy(F.col("count").desc()).show(truncate=False))

        # -------------------------------------------------
        # Invalid emails
        # -------------------------------------------------

        invalid_email_count = df.filter(
            F.col("email").isNotNull() & ~F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        ).count()

        print(f"\nInvalid email format: {invalid_email_count:,}")

        # -------------------------------------------------
        # Future signup dates
        # -------------------------------------------------

        future_signup_count = df.filter(F.col("signup_date") > F.current_date()).count()

        print(f"Future signup dates: {future_signup_count:,}")

        print()
        print("[PASS] USER PROFILING COMPLETED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
