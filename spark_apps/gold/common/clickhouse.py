import base64
import urllib.error
import urllib.request

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from spark_apps.gold.config.clickhouse import (
    CLICKHOUSE_DRIVER,
    CLICKHOUSE_HTTP_URL,
    CLICKHOUSE_JDBC_URL,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER,
)


def execute_clickhouse(
    sql: str,
) -> str:
    """
    Execute a ClickHouse SQL statement
    through the HTTP interface.
    """

    token = base64.b64encode(
        (
            f"{CLICKHOUSE_USER}:"
            f"{CLICKHOUSE_PASSWORD}"
        ).encode()
    ).decode()

    request = urllib.request.Request(
        CLICKHOUSE_HTTP_URL,
        data=sql.encode(),
        method="POST",
    )

    request.add_header(
        "Authorization",
        f"Basic {token}",
    )

    request.add_header(
        "Content-Type",
        "text/plain",
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=120,
        ) as response:

            return (
                response
                .read()
                .decode()
            )

    except urllib.error.HTTPError as exc:

        error_body = (
            exc
            .read()
            .decode(
                errors="replace"
            )
        )

        raise RuntimeError(
            "ClickHouse HTTP query failed:\n"
            f"{error_body}"
        ) from exc


def write_clickhouse(
    df: DataFrame,
    table: str,
) -> None:
    """
    Write Spark DataFrame into
    an existing ClickHouse table.
    """

    (
        df
        .coalesce(
            2
        )
        .write
        .format(
            "jdbc"
        )
        .option(
            "driver",
            CLICKHOUSE_DRIVER,
        )
        .option(
            "url",
            CLICKHOUSE_JDBC_URL,
        )
        .option(
            "user",
            CLICKHOUSE_USER,
        )
        .option(
            "password",
            CLICKHOUSE_PASSWORD,
        )
        .option(
            "dbtable",
            table,
        )
        .mode(
            "append"
        )
        .save()
    )


def read_clickhouse_table(
    spark: SparkSession,
    table: str,
) -> DataFrame:
    return (
        spark.read
        .format(
            "jdbc"
        )
        .option(
            "driver",
            CLICKHOUSE_DRIVER,
        )
        .option(
            "url",
            CLICKHOUSE_JDBC_URL,
        )
        .option(
            "user",
            CLICKHOUSE_USER,
        )
        .option(
            "password",
            CLICKHOUSE_PASSWORD,
        )
        .option(
            "dbtable",
            table,
        )
        .load()
    )
