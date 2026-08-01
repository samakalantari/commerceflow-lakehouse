from spark_apps.gold.common.clickhouse import execute_clickhouse
from spark_apps.gold.config.tables import (
    RETURN_REFUND_OBT_STAGING,
    TRANSACTIONAL_OBT_STAGING,
)


def main() -> None:
    staging_tables = [
        TRANSACTIONAL_OBT_STAGING,
        RETURN_REFUND_OBT_STAGING,
    ]

    for table in staging_tables:
        execute_clickhouse(
            f"TRUNCATE TABLE {table}"
        )
        print(f"[PASS] Staging table cleared: {table}")


if __name__ == "__main__":
    main()
