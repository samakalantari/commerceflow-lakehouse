from spark_apps.silver.config.iceberg import (
    ICEBERG_CATALOG_NAME,
    build_iceberg_spark,
)


SILVER_NAMESPACE = "silver"


def main() -> None:
    spark = build_iceberg_spark(
        "bootstrap-silver-iceberg"
    )

    catalog = ICEBERG_CATALOG_NAME

    namespace = (
        f"{catalog}.{SILVER_NAMESPACE}"
    )

    test_table = (
        f"{namespace}.__iceberg_smoke_test"
    )

    try:
        print("=" * 100)
        print("BOOTSTRAPPING SILVER ICEBERG")
        print("=" * 100)

        spark.sql(
            f"""
            CREATE NAMESPACE IF NOT EXISTS
            {namespace}
            """
        )

        print(
            f"[PASS] Namespace ready: "
            f"{namespace}"
        )

        spark.sql(
            f"""
            DROP TABLE IF EXISTS
            {test_table}
            """
        )

        spark.sql(
            f"""
            CREATE TABLE
            {test_table}
            (
                id BIGINT,
                value STRING,
                created_at TIMESTAMP
            )
            USING iceberg
            TBLPROPERTIES (
                'format-version' = '2'
            )
            """
        )

        print(
            "[PASS] Iceberg table created."
        )

        spark.sql(
            f"""
            INSERT INTO {test_table}
            VALUES (
                1,
                'initial-value',
                current_timestamp()
            )
            """
        )

        print(
            "[PASS] INSERT completed."
        )

        spark.sql(
            f"""
            MERGE INTO
                {test_table} AS target

            USING (
                SELECT
                    CAST(1 AS BIGINT) AS id,
                    'updated-value' AS value,
                    current_timestamp()
                        AS created_at
            ) AS source

            ON target.id = source.id

            WHEN MATCHED THEN
                UPDATE SET
                    target.value =
                        source.value,
                    target.created_at =
                        source.created_at

            WHEN NOT MATCHED THEN
                INSERT (
                    id,
                    value,
                    created_at
                )
                VALUES (
                    source.id,
                    source.value,
                    source.created_at
                )
            """
        )

        print(
            "[PASS] MERGE completed."
        )

        print("\nTABLE CONTENT")

        spark.table(
            test_table
        ).show(
            truncate=False
        )

        print("\nSNAPSHOTS")

        spark.sql(
            f"""
            SELECT
                snapshot_id,
                parent_id,
                operation,
                committed_at

            FROM
                {test_table}.snapshots

            ORDER BY
                committed_at
            """
        ).show(
            truncate=False
        )

        print()
        print("=" * 100)
        print(
            "SILVER ICEBERG FOUNDATION PASSED"
        )
        print("=" * 100)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
