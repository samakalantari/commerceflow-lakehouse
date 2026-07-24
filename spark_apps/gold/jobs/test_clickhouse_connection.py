import os

from pyspark.sql import SparkSession

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "gold")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

JDBC_URL = f"jdbc:ch://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}"

DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"
TABLE = "spark_connection_test"


def main():
    spark = SparkSession.builder.appName("Gold-ClickHouse-Connection-Test").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=== Creating test DataFrame ===")

    data = [
        (1, "spark-test-a"),
        (2, "spark-test-b"),
    ]

    df = spark.createDataFrame(
        data,
        ["id", "name"],
    )

    df.show()

    print("=== Writing DataFrame to ClickHouse ===")

    (
        df.write.format("jdbc")
        .option("driver", DRIVER)
        .option("url", JDBC_URL)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("dbtable", TABLE)
        .mode("append")
        .save()
    )

    print("=== Write successful ===")

    print("=== Reading data back from ClickHouse ===")

    result_df = (
        spark.read.format("jdbc")
        .option("driver", DRIVER)
        .option("url", JDBC_URL)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("dbtable", TABLE)
        .load()
    )

    result_df.show(truncate=False)

    print("=== ClickHouse JDBC test completed successfully ===")

    spark.stop()


if __name__ == "__main__":
    main()
