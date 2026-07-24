from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)


DAG_ID = (
    "behavioral_silver_etl"
)

SPARK_CONN_ID = (
    "spark_standalone"
)

SPARK_PACKAGES = ",".join(
    [
        (
            "org.apache.iceberg:"
            "iceberg-spark-runtime-3.5_2.12:1.11.0"
        ),
        (
            "org.postgresql:"
            "postgresql:42.7.13"
        ),
    ]
)


DEFAULT_ARGS = {
    "owner": "group4",

    "depends_on_past": False,

    "retries": 1,

    "retry_delay": timedelta(
        minutes=5
    ),
}


COMMON_SPARK_CONF = {
    # The Spark driver runs in the Airflow scheduler
    # container.
    "spark.driver.host": (
        "airflow-scheduler"
    ),

    "spark.driver.bindAddress": (
        "0.0.0.0"
    ),

    # Maven/Ivy dependency cache.
    "spark.jars.ivy": (
        "/tmp/.ivy2"
    ),

    # Python configuration for Spark executors.
    "spark.executorEnv.PYTHONPATH": (
        "/opt/project"
    ),

    "spark.executorEnv.PYTHONDONTWRITEBYTECODE": (
        "1"
    ),

    # Limit this ETL to two total Spark cores.
    "spark.cores.max": (
        "2"
    ),

    "spark.executor.cores": (
        "1"
    ),

    "spark.executor.memory": (
        "1g"
    ),

    # All event-time intervals use UTC.
    "spark.sql.session.timeZone": (
        "UTC"
    ),

    "spark.ui.showConsoleProgress": (
        "false"
    ),
}


DRIVER_ENV_VARS = {
    "PYTHONPATH": (
        "/opt/project"
    ),

    "PYTHONDONTWRITEBYTECODE": (
        "1"
    ),
}


with DAG(
    dag_id=DAG_ID,

    description=(
        "Behavioral Bronze-to-Silver ETL "
        "using event time"
    ),

    # Historical Behavioral data starts on July 12.
    start_date=pendulum.datetime(
        2026,
        7,
        12,
        0,
        0,
        tz="UTC",
    ),

    # Run every three hours in UTC.
    schedule=(
        "0 */3 * * *"
    ),

    # Recover historical and missed intervals.
    catchup=False,

    # Run only one Behavioral ETL job at a time.
    max_active_runs=1,

    default_args=DEFAULT_ARGS,

    tags=[
        "behavioral",
        "silver",
        "spark",
        "iceberg",
        "event-time",
    ],
) as dag:

    load_fact_behavioral_event = (
        SparkSubmitOperator(
            task_id=(
                "load_fact_behavioral_event"
            ),

            conn_id=SPARK_CONN_ID,

            application=(
                "/opt/project/"
                "spark_apps/silver/jobs/"
                "load_fact_behavioral_event.py"
            ),

            packages=(
                SPARK_PACKAGES
            ),

            conf=(
                COMMON_SPARK_CONF
            ),

            env_vars=(
                DRIVER_ENV_VARS
            ),

            name=(
                "behavioral-silver-event-time-etl"
            ),

            deploy_mode=(
                "client"
            ),

            verbose=False,

            # A manual DAG run can provide:
            #
            # {
            #   "start_ts": "2026-07-12 00:00:00",
            #   "end_ts":   "2026-07-13 00:00:00"
            # }
            #
            # A scheduled run uses the normal Airflow
            # event-time interval.
            application_args=[
                "--start-ts",

                (
                    "{{ "
                    "dag_run.conf.get('start_ts') "
                    "or "
                    "data_interval_start"
                    ".in_timezone('UTC')"
                    ".strftime("
                    "'%Y-%m-%d %H:%M:%S'"
                    ") "
                    "}}"
                ),

                "--end-ts",

                (
                    "{{ "
                    "dag_run.conf.get('end_ts') "
                    "or "
                    "data_interval_end"
                    ".in_timezone('UTC')"
                    ".strftime("
                    "'%Y-%m-%d %H:%M:%S'"
                    ") "
                    "}}"
                ),
            ],

            execution_timeout=timedelta(
                minutes=90
            ),
        )
    )