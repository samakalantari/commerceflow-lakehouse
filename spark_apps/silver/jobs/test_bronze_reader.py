from spark_apps.silver.common.bronze_reader import (
    bronze_topic_path,
    read_bronze_topic,
)
from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    TOPIC_USERS,
)


def main() -> None:
    spark = build_iceberg_spark(
        "test-silver-bronze-reader"
    )

    try:
        path = bronze_topic_path(
            TOPIC_USERS
        )

        print("=" * 100)
        print("SILVER BRONZE READER TEST")
        print("=" * 100)

        print(
            f"Topic: {TOPIC_USERS}"
        )

        print(
            f"Path: {path}"
        )

        df = read_bronze_topic(
            spark,
            TOPIC_USERS,
        )

        count = df.count()

        print(
            f"Records: {count:,}"
        )

        print("\nSchema:")
        df.printSchema()

        print("\nSample:")

        df.limit(
            3
        ).show(
            truncate=False,
            vertical=True,
        )

        print()
        print(
            "[PASS] Bronze reader works."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
