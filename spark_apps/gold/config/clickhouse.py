import os


def get_required_env(
    name: str,
) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(f"Required environment variable '{name}' is not set.")

    return value


CLICKHOUSE_HOST = os.getenv(
    "CLICKHOUSE_HOST",
    "clickhouse",
)

CLICKHOUSE_PORT = os.getenv(
    "CLICKHOUSE_PORT",
    "8123",
)

CLICKHOUSE_DATABASE = os.getenv(
    "CLICKHOUSE_DATABASE",
    "gold",
)

CLICKHOUSE_USER = get_required_env("CLICKHOUSE_USER")

CLICKHOUSE_PASSWORD = get_required_env("CLICKHOUSE_PASSWORD")


CLICKHOUSE_JDBC_URL = f"jdbc:ch://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}"

CLICKHOUSE_HTTP_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"

CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"
