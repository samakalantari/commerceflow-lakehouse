from datetime import timedelta

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)

from airflow import DAG

DAG_ID = "gold_transactional_etl"

SPARK_CONN_ID = "spark_standalone"


# ============================================================
# Spark Packages
#
# clickhouse-jdbc-all is the same shaded artifact
# already verified successfully in this environment.
# ============================================================

SPARK_PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0",
        "org.postgresql:postgresql:42.7.13",
        "com.clickhouse:clickhouse-jdbc-all:0.9.8",
    ]
)


COMMON_SPARK_CONF = {
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.jars.ivy": "/tmp/.ivy2",
    "spark.pyspark.driver.python": "python3",
    "spark.executorEnv.PYSPARK_PYTHON": "/opt/bitnami/python/bin/python3",
    "spark.executorEnv.PYTHONPATH": "/opt/project",
    "spark.executorEnv.PYTHONDONTWRITEBYTECODE": "1",
    # Two workers x two cores
    "spark.cores.max": "4",
    "spark.executor.cores": "2",
    # Dataset is moderate;
    # avoid excessive tiny shuffle tasks.
    "spark.sql.shuffle.partitions": "8",
}


COMMON_ENV_VARS = {
    "PYTHONPATH": "/opt/project",
    "PYTHONDONTWRITEBYTECODE": "1",
    "PYSPARK_DRIVER_PYTHON": "python3",
}


DEFAULT_ARGS = {
    "owner": "group4",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def gold_spark_task(
    task_id: str,
    application: str,
) -> SparkSubmitOperator:

    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=SPARK_CONN_ID,
        application=application,
        packages=SPARK_PACKAGES,
        conf=COMMON_SPARK_CONF,
        env_vars=COMMON_ENV_VARS,
        deploy_mode="client",
        verbose=False,
    )


with DAG(
    dag_id=DAG_ID,
    description=("Transactional Silver-to-Gold ETL building a ClickHouse OBT"),
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(
        2026,
        7,
        23,
        tz="UTC",
    ),
    # Gold is triggered after Silver succeeds.
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=[
        "gold",
        "transactional",
        "spark",
        "clickhouse",
        "obt",
    ],
) as dag:
    load_transactional_obt = gold_spark_task(
        task_id=("load_transactional_obt"),
        application=("/opt/project/spark_apps/gold/jobs/load_transactional_obt.py"),
    )

    audit_gold = gold_spark_task(
        task_id=("audit_gold"),
        application=("/opt/project/spark_apps/gold/jobs/audit_gold.py"),
    )

    (load_transactional_obt >> audit_gold)
