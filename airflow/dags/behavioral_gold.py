from __future__ import annotations


import os
import re
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

TEHRAN_TZ = pendulum.timezone("Asia/Tehran")


# --------------------------------------------------------------------------
# Env helpers -- no hardcoded secrets, ever. Required vars raise loudly.
# --------------------------------------------------------------------------
def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _env(name: str, default: str) -> str:
    """Non-secret config with a sane default (ports, db/catalog names)."""
    return os.getenv(name, default)


def _to_s3a(uri: str) -> str:
    uri = uri.strip()
    if uri.startswith("s3://"):
        return "s3a://" + uri[len("s3://"):]
    return uri


# --------------------------------------------------------------------------
# Required secrets / endpoints -- pulled straight from your .env via the
# Airflow container's environment. None of these have hardcoded fallbacks.
# --------------------------------------------------------------------------
MINIO_ENDPOINT = _require_env("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = _require_env("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = _require_env("MINIO_SECRET_KEY")

ICEBERG_WAREHOUSE = _to_s3a(_require_env("ICEBERG_WAREHOUSE"))
ICEBERG_CATALOG_NAME = _env("ICEBERG_CATALOG_NAME", "lakehouse")

# Your Iceberg catalog is JDBC-backed (Postgres) per .env -- pass these
# through so build_iceberg_spark()/the JDBC catalog config can use them.
ICEBERG_JDBC_URI = _require_env("ICEBERG_JDBC_URI")
ICEBERG_JDBC_USER = _require_env("ICEBERG_JDBC_USER")
ICEBERG_JDBC_PASSWORD = _require_env("ICEBERG_JDBC_PASSWORD")

CLICKHOUSE_HOST = _env("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_HTTP_PORT = _env("CLICKHOUSE_HTTP_PORT", "8123")
CLICKHOUSE_NATIVE_PORT = _env("CLICKHOUSE_NATIVE_PORT", "9000")
CLICKHOUSE_USER = _require_env("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = _require_env("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB = _env("CLICKHOUSE_DB", "gold")

MAX_DAYS_PER_RUN = _env("MAX_DAYS_PER_RUN", "35")
SPARK_SHUFFLE_PARTITIONS = _env("SPARK_SHUFFLE_PARTITIONS", "8")
CH_WRITE_NUM_PARTITIONS = _env("CH_WRITE_NUM_PARTITIONS", "8")
CH_WRITE_BATCHSIZE = _env("CH_WRITE_BATCHSIZE", "100000")

# --------------------------------------------------------------------------
# Spark resource limits -- defined here, in the DAG, on purpose (not in
# .env) so this DAG's footprint is capped regardless of what anyone sets
# cluster-wide. Edit these constants directly to change the limit.
#   SPARK_CORES_MAX      -- total cores across all executors for the app
#   SPARK_EXECUTOR_CORES -- cores per executor
#   SPARK_EXECUTOR_MEMORY-- memory per executor
#   SPARK_DRIVER_MEMORY  -- driver (client-mode, runs on airflow-scheduler)
# --------------------------------------------------------------------------
SPARK_CORES_MAX = "2"
SPARK_EXECUTOR_CORES = "1"
SPARK_EXECUTOR_MEMORY = "1g"
SPARK_DRIVER_MEMORY = "1g"

# --------------------------------------------------------------------------
# Jars -- pre-installed only, nothing fetched at run time.
#
# IMPORTANT (disk-fill incident, 2026-08-01): aws-java-sdk-bundle-1.12.262.jar
# (~280MB) and hadoop-aws-3.3.4.jar were REMOVED from this list. They are
# already baked into /opt/bitnami/spark/jars on every node -- driver
# (airflow-scheduler) AND workers, since both images derive from the same
# bitnami/spark:3.5 base. Passing them via --jars anyway doesn't check
# "is this already on the classpath" -- it unconditionally restages the
# file into a fresh work/app-<id>/ directory on every worker, for every
# single Spark submission. With this DAG's schedule (4 tasks x up to
# 4 runs/day, or up to 304 submissions across a full backfill), that
# duplication filled the host disk. The ORIGINAL pre-unification DAGs
# (entity_daily_etl, session_etl, user_daily_etl) never shipped these two
# jars at all -- proof they were never actually needed; s3a/HadoopFileIO
# resolves them from the node's own default classpath automatically.
#
# Only the 3 jars NOT part of the stock image are still shipped:
#   - iceberg-spark-runtime-3.5_2.12-1.11.0.jar and
#     clickhouse-jdbc-all-0.9.8.jar exist only because a PREVIOUS run of
#     the old packages=/ivy DAGs downloaded them into
#     /tmp/airflow-ivy-behavioral-gold/cache/... . This is NOT a stable
#     install location -- /tmp is very likely wiped on container
#     recreation/restart. Fine for now; before relying on this long-term,
#     copy these two jars into /opt/bitnami/spark/jars (or bake them into
#     the image) so they survive a restart.
#   - postgresql-42.7.13.jar: confirmed in the same ivy cache -- same
#     /tmp fragility caveat as the two jars above applies to this one too.
#
# Even with worker-side cleanup still off, dropping these two cuts the
# per-submission disk footprint by roughly 80%+ on its own.
# --------------------------------------------------------------------------
SPARK_JARS_DIR = _env("SPARK_JARS_DIR", "/opt/bitnami/spark/jars")
IVY_CACHE_DIR = _env(
    "GOLD_SPARK_IVY_CACHE_DIR",
    "/tmp/airflow-ivy-behavioral-gold/cache",
)
POSTGRES_JAR_PATH = _env(
    "GOLD_SPARK_POSTGRES_JAR",
    f"{IVY_CACHE_DIR}/org.postgresql/postgresql/jars/postgresql-42.7.13.jar",
)

_JARS = [
    f"{IVY_CACHE_DIR}/org.apache.iceberg/iceberg-spark-runtime-3.5_2.12/jars/iceberg-spark-runtime-3.5_2.12-1.11.0.jar",
    f"{IVY_CACHE_DIR}/com.clickhouse/clickhouse-jdbc-all/jars/clickhouse-jdbc-all-0.9.8.jar",
]
if POSTGRES_JAR_PATH:
    _JARS.append(POSTGRES_JAR_PATH)

EXTRA_JARS = [j.strip() for j in _env("GOLD_SPARK_EXTRA_JARS", "").split(",") if j.strip()]
JARS = ",".join(_JARS + EXTRA_JARS)

# --------------------------------------------------------------------------
# Shared Spark conf -- catalog + s3a wiring.
#
# This mirrors spark_apps/silver/config/iceberg.py::build_iceberg_spark()
# exactly, which is the function load_gold_behavioral_entity_daily.py,
# load_gold_behavioral_session.py, and load_gold_behavioral_user_daily.py
# actually call to get their catalog set up:
#   - catalog type = "jdbc" (Iceberg's built-in shorthand, not catalog-impl)
#   - io-impl = HadoopFileIO -> Iceberg reads/writes data files through
#     Hadoop's generic s3a:// filesystem, driven by spark.hadoop.fs.s3a.*
#     below -- NOT through catalog-level spark.sql.catalog.<name>.s3.*
#     keys, which only apply to S3FileIO and are omitted here on purpose.
#
# IMPORTANT: load_gold_behavioral_daily.py does NOT call
# build_iceberg_spark() -- it builds a bare SparkSession and depends
# entirely on catalog config already being present via spark-submit
# --conf. That means this dict is the ONLY thing setting up the catalog
# for that job, so it must stay in lockstep with build_iceberg_spark().
# --------------------------------------------------------------------------
COMMON_SPARK_CONF = {
    "spark.cores.max": SPARK_CORES_MAX,
    "spark.executor.cores": SPARK_EXECUTOR_CORES,
    "spark.executor.memory": SPARK_EXECUTOR_MEMORY,
    "spark.driver.memory": SPARK_DRIVER_MEMORY,
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.shuffle.partitions": SPARK_SHUFFLE_PARTITIONS,
    "spark.sql.session.timeZone": "UTC",
    "spark.ui.showConsoleProgress": "false",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}": "org.apache.iceberg.spark.SparkCatalog",
    f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.type": "jdbc",
    f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.uri": ICEBERG_JDBC_URI,
    f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.jdbc.user": ICEBERG_JDBC_USER,
    f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.jdbc.password": ICEBERG_JDBC_PASSWORD,
    f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.warehouse": ICEBERG_WAREHOUSE,
    f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
    "spark.driver.extraJavaOptions": "-Daws.region=us-east-1",
    "spark.executor.extraJavaOptions": "-Daws.region=us-east-1",
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.executorEnv.PYTHONPATH": "/opt/project",
    "spark.executorEnv.PYTHONDONTWRITEBYTECODE": "1",
}

COMMON_ENV_VARS = {
    "PYTHONPATH": "/opt/project",
    "PYTHONDONTWRITEBYTECODE": "1",
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_REGION": "us-east-1",
    "AWS_DEFAULT_REGION": "us-east-1",
    "ICEBERG_WAREHOUSE": ICEBERG_WAREHOUSE,
    "ICEBERG_CATALOG_NAME": ICEBERG_CATALOG_NAME,
    "ICEBERG_JDBC_URI": ICEBERG_JDBC_URI,
    "ICEBERG_JDBC_USER": ICEBERG_JDBC_USER,
    "ICEBERG_JDBC_PASSWORD": ICEBERG_JDBC_PASSWORD,
    "CLICKHOUSE_HOST": CLICKHOUSE_HOST,
    "CLICKHOUSE_HTTP_PORT": CLICKHOUSE_HTTP_PORT,
    "CLICKHOUSE_NATIVE_PORT": CLICKHOUSE_NATIVE_PORT,
    "CLICKHOUSE_USER": CLICKHOUSE_USER,
    "CLICKHOUSE_PASSWORD": CLICKHOUSE_PASSWORD,
    "CLICKHOUSE_DB": CLICKHOUSE_DB,
    "MAX_DAYS_PER_RUN": MAX_DAYS_PER_RUN,
    "CH_WRITE_NUM_PARTITIONS": CH_WRITE_NUM_PARTITIONS,
    "CH_WRITE_BATCHSIZE": CH_WRITE_BATCHSIZE,
    "SPARK_SHUFFLE_PARTITIONS": SPARK_SHUFFLE_PARTITIONS,
}


def validate_runtime(**context):
    """Sanity-check the 6h data interval this run is processing."""
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]

    if data_interval_start >= data_interval_end:
        raise ValueError(
            f"data_interval_start >= data_interval_end: "
            f"{data_interval_start} >= {data_interval_end}"
        )

    activity_date = data_interval_start.in_timezone("Asia/Tehran").strftime("%Y-%m-%d")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", activity_date):
        raise ValueError(f"Invalid derived activity_date: {activity_date}")

    print(
        f"Validated window: {data_interval_start} -> {data_interval_end} "
        f"(activity_date={activity_date})"
    )


default_args = {
    "owner": "group4",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="behavioral_gold_etl",
    description="Unified 6-hourly ETL for all gold_behavioral_* ClickHouse tables",
    default_args=default_args,
    start_date=datetime(2026, 1, 1, tzinfo=TEHRAN_TZ),
    schedule="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,  # sequential -- small Spark cluster (2 cores)
    tags=["gold", "behavioral", "clickhouse"],
) as dag:

    validate = PythonOperator(
        task_id="validate_runtime",
        python_callable=validate_runtime,
    )

    load_daily = SparkSubmitOperator(
        task_id="load_gold_behavioral_daily",
        application="/opt/project/spark_apps/gold/jobs/load_gold_behavioral_daily.py",
        conn_id="spark_standalone",
        name="load_gold_behavioral_daily",
        verbose=False,
        jars=JARS,
        conf=COMMON_SPARK_CONF,
        env_vars=COMMON_ENV_VARS,
        application_args=[
            "--start-date",
            "{{ data_interval_start.in_timezone('Asia/Tehran').strftime('%Y-%m-%d') }}",
            "--end-date",
            "{{ data_interval_start.in_timezone('Asia/Tehran').strftime('%Y-%m-%d') }}",
        ],
        execution_timeout=timedelta(minutes=60),
    )

    load_user_daily = SparkSubmitOperator(
        task_id="load_gold_behavioral_user_daily",
        application="/opt/project/spark_apps/gold/jobs/load_gold_behavioral_user_daily.py",
        conn_id="spark_standalone",
        name="load_gold_behavioral_user_daily",
        verbose=False,
        jars=JARS,
        conf=COMMON_SPARK_CONF,
        env_vars=COMMON_ENV_VARS,
        application_args=[
            "--activity-date",
            "{{ data_interval_start.in_timezone('Asia/Tehran').strftime('%Y-%m-%d') }}",
        ],
        execution_timeout=timedelta(minutes=60),
    )


    load_session = SparkSubmitOperator(
        task_id="load_gold_behavioral_session",
        application="/opt/project/spark_apps/gold/jobs/load_gold_behavioral_session.py",
        conn_id="spark_standalone",
        name="load_gold_behavioral_session",
        verbose=False,
        jars=JARS,
        conf=COMMON_SPARK_CONF,
        env_vars=COMMON_ENV_VARS,
        application_args=[
            "--start-date",
            "{{ data_interval_start.in_timezone('UTC').strftime('%Y-%m-%d %H:%M:%S') }}",
            "--end-date-exclusive",
            "{{ data_interval_end.in_timezone('UTC').strftime('%Y-%m-%d %H:%M:%S') }}",
        ],
        execution_timeout=timedelta(hours=2),
    )

    validate >> load_daily >> load_user_daily  >> load_session