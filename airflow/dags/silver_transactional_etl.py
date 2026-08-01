from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from typing import List, Optional
from datetime import timedelta

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)

from airflow import DAG

# ============================================================
# General Configuration
# ============================================================

DAG_ID = "silver_transactional_etl"

# Airflow Spark Connection
#
# Expected connection URI:
# spark://spark-master:7077
#
# Configure in airflow-scheduler:
#
# AIRFLOW_CONN_SPARK_STANDALONE=spark://spark-master:7077
#
SPARK_CONN_ID = "spark_standalone"


# ============================================================
# Spark Packages
# ============================================================

SPARK_PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0",
        "org.postgresql:postgresql:42.7.13",
    ]
)


# ============================================================
# Common Spark Configuration
# ============================================================

COMMON_SPARK_CONF = {
    # --------------------------------------------------------
    # Driver Networking
    #
    # Spark driver runs inside airflow-scheduler container.
    # Spark workers must be able to connect back to it.
    # --------------------------------------------------------
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    # --------------------------------------------------------
    # Ivy / Maven Package Cache
    # --------------------------------------------------------
    "spark.jars.ivy": "/tmp/.ivy2",
    # --------------------------------------------------------
    # Python Driver
    #
    # Driver runs inside Airflow container,
    # therefore we must NOT use:
    #
    # /opt/bitnami/python/bin/python3
    #
    # here.
    # --------------------------------------------------------
    "spark.pyspark.driver.python": "python3",
    # --------------------------------------------------------
    # Spark Executor Environment
    #
    # Executors run inside Bitnami Spark workers.
    # --------------------------------------------------------
    "spark.executorEnv.PYSPARK_PYTHON": "/opt/bitnami/python/bin/python3",
    "spark.executorEnv.PYTHONPATH": "/opt/project",
    "spark.executorEnv.PYTHONDONTWRITEBYTECODE": "1",
    "spark.cores.max": "1",
    "spark.executor.cores": "1",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "16",
}


# ============================================================
# Driver Environment
#
# These environment variables are applied to spark-submit
# running inside airflow-scheduler.
#
# MinIO / Iceberg / Bronze environment variables are expected
# to already exist in airflow-scheduler.environment and will
# therefore be inherited by the spark-submit process.
# ============================================================

COMMON_ENV_VARS = {
    "PYTHONPATH": "/opt/project",
    "PYTHONDONTWRITEBYTECODE": "1",
    "PYSPARK_DRIVER_PYTHON": "python3",
}


# ============================================================
# Default Airflow Task Arguments
# ============================================================

DEFAULT_ARGS = {
    "owner": "group4",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# Helper Function
# ============================================================


def silver_spark_task(
    task_id: str,
    application: str,
    *,
    application_args: Optional[List[str]] = None,
) -> SparkSubmitOperator:
    """
    Create a SparkSubmitOperator with the common configuration
    used by all Transactional Silver jobs.
    """

    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=SPARK_CONN_ID,
        application=application,
        application_args=application_args,
        packages=SPARK_PACKAGES,
        conf=COMMON_SPARK_CONF,
        env_vars=COMMON_ENV_VARS,
        deploy_mode="client",
        verbose=False,
    )


# ============================================================
# DAG Definition
# ============================================================

with DAG(
    dag_id=DAG_ID,
    description=(
        "Transactional Bronze-to-Silver ETL pipeline using Spark, MinIO and Apache Iceberg"
    ),
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(
        2026,
        7,
        17,
        tz="UTC",
    ),
    # Each run processes the completed UTC Bronze ingestion-date directory.
    schedule="@daily",
    catchup=False,
    # Prevent two Silver pipelines from writing concurrently
    max_active_runs=1,
    tags=[
        "silver",
        "transactional",
        "spark",
        "minio",
        "iceberg",
    ],
) as dag:
    source_args = [
        "--source-mode",
        "{{ dag_run.conf.get('source_mode', 'daily') if dag_run.conf else 'daily' }}",
        "--ingested-date",
        "{{ data_interval_start.in_timezone('UTC').strftime('%Y-%m-%d') }}",
    ]

    # ========================================================
    # 1. Bootstrap Silver / Iceberg
    #
    # Ensures:
    # - Iceberg catalog connectivity
    # - Silver namespace
    # - Required Iceberg foundation
    # ========================================================

    bootstrap_silver = silver_spark_task(
        task_id="bootstrap_silver",
        application=("/opt/project/spark_apps/silver/jobs/bootstrap_silver.py"),
    )

    # ========================================================
    # 2. Dimension: Date
    #
    # Reads source dates from Bronze orders in MinIO
    # and ensures dim_date contains the required date range.
    # ========================================================

    load_dim_date = silver_spark_task(
        task_id="load_dim_date",
        application=("/opt/project/spark_apps/silver/jobs/load_dim_date.py"),
        application_args=source_args,
    )

    # ========================================================
    # 3. Dimension: User
    #
    # Bronze MinIO
    #     transactional.users
    #          ↓
    # Spark clean / validate
    #          ↓
    # Silver Iceberg dim_user
    # ========================================================

    load_dim_user = silver_spark_task(
        task_id="load_dim_user",
        application=("/opt/project/spark_apps/silver/jobs/load_dim_user.py"),
        application_args=source_args,
    )

    # ========================================================
    # 4. Dimension: Product - SCD Type 2
    #
    # Bronze:
    # - products
    # - product_price_history
    #
    # Builds SCD2 history in Silver Iceberg.
    # ========================================================

    load_dim_product = silver_spark_task(
        task_id="load_dim_product",
        application=("/opt/project/spark_apps/silver/jobs/load_dim_product.py"),
        application_args=source_args,
    )

    # ========================================================
    # 5. Fact: Order
    #
    # Bronze orders
    #      ↓
    # Resolve user/date dimensions
    #      ↓
    # Silver Iceberg fact_order
    # ========================================================

    load_fact_order = silver_spark_task(
        task_id="load_fact_order",
        application=("/opt/project/spark_apps/silver/jobs/load_fact_order.py"),
        application_args=source_args,
    )

    # ========================================================
    # 6. Fact: Order Item
    #
    # Bronze order_items
    #      ↓
    # Resolve order
    #      ↓
    # Temporal SCD2 product lookup
    #      ↓
    # Earliest-version fallback when required
    #      ↓
    # Silver Iceberg fact_order_item
    # ========================================================

    load_fact_order_item = silver_spark_task(
        task_id="load_fact_order_item",
        application=("/opt/project/spark_apps/silver/jobs/load_fact_order_item.py"),
        application_args=source_args,
    )

    # ========================================================
    # 7. Fact: Return and Refund
    # ========================================================

    load_fact_return_refund = silver_spark_task(
        task_id="load_fact_return_refund",
        application=(
            "/opt/project/spark_apps/silver/jobs/load_fact_return_refund.py"
        ),
        application_args=source_args,
    )

    # ========================================================
    # 8. Silver End-to-End Audit
    #
    # Validates:
    # - dimensions
    # - facts
    # - uniqueness
    # - foreign keys
    # - SCD2 integrity
    # - product resolution
    # - Iceberg snapshots
    # ========================================================

    audit_silver = silver_spark_task(
        task_id="audit_silver",
        application=("/opt/project/spark_apps/silver/jobs/audit_silver.py"),
    )

    #9. Trigger Gold Layer
    trigger_transactional_gold = TriggerDagRunOperator(
    task_id="trigger_transactional_gold",
    trigger_dag_id="gold_transactional_etl",

    conf={
        "triggered_by": "silver_transactional_etl",
        "silver_run_id": "{{ run_id }}",
        "ingested_date": (
            "{{ data_interval_start.in_timezone('UTC').strftime('%Y-%m-%d') }}"
        ),
    },

    wait_for_completion=False,

    retries=0,
    )


    # ========================================================
    # Pipeline Dependency Graph
    # ========================================================

    (
        bootstrap_silver
        >> load_dim_date
        >> load_dim_user
        >> load_dim_product
        >> load_fact_order
        >> load_fact_order_item
        >> load_fact_return_refund
        >> audit_silver
        >> trigger_transactional_gold
    )
