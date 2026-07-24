from unittest.mock import MagicMock

import pytest

from spark_apps.bronze.config.minio import configure_minio_storage


@pytest.fixture
def mock_spark():
    spark = MagicMock()

    hadoop_conf = MagicMock()
    spark.sparkContext._jsc.hadoopConfiguration.return_value = hadoop_conf

    return spark, hadoop_conf


def test_configure_minio_storage_raises_when_all_variables_are_missing(
    monkeypatch,
    mock_spark,
):
    spark, _ = mock_spark

    monkeypatch.delenv("MINIO_ENDPOINT", raising=False)
    monkeypatch.delenv("MINIO_ACCESS_KEY", raising=False)
    monkeypatch.delenv("MINIO_SECRET_KEY", raising=False)

    with pytest.raises(
        RuntimeError,
        match="Missing MinIO environment variables",
    ):
        configure_minio_storage(spark)


def test_configure_minio_storage_lists_missing_variables(
    monkeypatch,
    mock_spark,
):
    spark, _ = mock_spark

    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.delenv("MINIO_ACCESS_KEY", raising=False)
    monkeypatch.delenv("MINIO_SECRET_KEY", raising=False)

    with pytest.raises(RuntimeError) as exc_info:
        configure_minio_storage(spark)

    error_message = str(exc_info.value)

    assert "MINIO_ACCESS_KEY" in error_message
    assert "MINIO_SECRET_KEY" in error_message
    assert "MINIO_ENDPOINT" not in error_message


def test_configure_minio_storage_sets_hadoop_configuration(
    monkeypatch,
    mock_spark,
):
    spark, hadoop_conf = mock_spark

    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "spark-user")
    monkeypatch.setenv("MINIO_SECRET_KEY", "spark-secret")

    configure_minio_storage(spark)

    expected_calls = [
        (
            "fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        ),
        (
            "fs.s3a.endpoint",
            "http://minio:9000",
        ),
        (
            "fs.s3a.access.key",
            "spark-user",
        ),
        (
            "fs.s3a.secret.key",
            "spark-secret",
        ),
        (
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        ),
        (
            "fs.s3a.path.style.access",
            "true",
        ),
        (
            "fs.s3a.connection.ssl.enabled",
            "false",
        ),
    ]

    actual_calls = [call.args for call in hadoop_conf.set.call_args_list]

    for expected_call in expected_calls:
        assert expected_call in actual_calls


def test_configure_minio_storage_enables_ssl_for_https_endpoint(
    monkeypatch,
    mock_spark,
):
    spark, hadoop_conf = mock_spark

    monkeypatch.setenv(
        "MINIO_ENDPOINT",
        "https://storage.example.com",
    )
    monkeypatch.setenv("MINIO_ACCESS_KEY", "access-key")
    monkeypatch.setenv("MINIO_SECRET_KEY", "secret-key")

    configure_minio_storage(spark)

    hadoop_conf.set.assert_any_call(
        "fs.s3a.connection.ssl.enabled",
        "true",
    )


def test_configure_minio_storage_disables_ssl_for_http_endpoint(
    monkeypatch,
    mock_spark,
):
    spark, hadoop_conf = mock_spark

    monkeypatch.setenv(
        "MINIO_ENDPOINT",
        "http://minio:9000",
    )
    monkeypatch.setenv("MINIO_ACCESS_KEY", "access-key")
    monkeypatch.setenv("MINIO_SECRET_KEY", "secret-key")

    configure_minio_storage(spark)

    hadoop_conf.set.assert_any_call(
        "fs.s3a.connection.ssl.enabled",
        "false",
    )
