import os

from pyspark.sql import SparkSession

from spark_apps.bronze.config.minio import configure_minio_storage

ICEBERG_CATALOG_NAME = os.getenv(
    "ICEBERG_CATALOG_NAME",
    "lakehouse",
)

ICEBERG_WAREHOUSE = os.getenv(
    "ICEBERG_WAREHOUSE",
    "s3a://commerceflow-lakehouse/silver",
)


def get_required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(f"Required environment variable '{name}' is not set.")

    return value


def build_iceberg_spark(
    app_name: str,
) -> SparkSession:

    catalog = ICEBERG_CATALOG_NAME

    jdbc_uri = get_required_env("ICEBERG_JDBC_URI")

    jdbc_user = get_required_env("ICEBERG_JDBC_USER")

    jdbc_password = get_required_env("ICEBERG_JDBC_PASSWORD")

    spark = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{catalog}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            f"spark.sql.catalog.{catalog}.type",
            "jdbc",
        )
        .config(
            f"spark.sql.catalog.{catalog}.uri",
            jdbc_uri,
        )
        .config(
            f"spark.sql.catalog.{catalog}.jdbc.user",
            jdbc_user,
        )
        .config(
            f"spark.sql.catalog.{catalog}.jdbc.password",
            jdbc_password,
        )
        .config(
            f"spark.sql.catalog.{catalog}.warehouse",
            ICEBERG_WAREHOUSE,
        )
        .config(
            f"spark.sql.catalog.{catalog}.io-impl",
            "org.apache.iceberg.hadoop.HadoopFileIO",
        )
        .getOrCreate()
    )

    configure_minio_storage(spark)

    spark.sparkContext.setLogLevel("WARN")

    return spark
