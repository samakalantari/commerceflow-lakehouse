from spark_apps.gold.config.clickhouse import (
    CLICKHOUSE_DATABASE,
)


TRANSACTIONAL_OBT = (
    f"{CLICKHOUSE_DATABASE}."
    f"transactional_obt"
)

TRANSACTIONAL_OBT_STAGING = (
    f"{CLICKHOUSE_DATABASE}."
    f"transactional_obt_staging"
)
