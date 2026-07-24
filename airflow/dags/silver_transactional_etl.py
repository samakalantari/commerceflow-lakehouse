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
    "spark.cores.max": "2",
    "spark.executor.cores": "2",
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
) -> SparkSubmitOperator:
    """
    Create a SparkSubmitOperator with the common configuration
    used by all Transactional Silver jobs.
    """

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
    # Run once per day
    # schedule="@daily",
    schedule="0 */6 * * *",
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
    )

    # ========================================================
    # 7. Silver End-to-End Audit
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
        >> audit_silver
    )
