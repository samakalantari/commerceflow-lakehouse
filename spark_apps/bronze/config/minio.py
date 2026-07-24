import os

from pyspark.sql import SparkSession


def configure_minio_storage(spark: SparkSession) -> None:
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    missing_variables = [
        variable_name
        for variable_name, value in {
            "MINIO_ENDPOINT": endpoint,
            "MINIO_ACCESS_KEY": access_key,
            "MINIO_SECRET_KEY": secret_key,
        }.items()
        if not value
    ]

    if missing_variables:
        missing = ", ".join(missing_variables)
        raise RuntimeError(f"Missing MinIO environment variables: {missing}")

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    hadoop_conf.set(
        "fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )

    hadoop_conf.set(
        "fs.s3a.endpoint",
        endpoint,
    )

    hadoop_conf.set(
        "fs.s3a.access.key",
        access_key,
    )

    hadoop_conf.set(
        "fs.s3a.secret.key",
        secret_key,
    )

    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )

    hadoop_conf.set(
        "fs.s3a.path.style.access",
        "true",
    )

    hadoop_conf.set(
        "fs.s3a.connection.ssl.enabled",
        str(endpoint.startswith("https://")).lower(),
    )
