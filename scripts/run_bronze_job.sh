#!/usr/bin/env bash

set -euo pipefail

readonly PROJECT_ROOT="/opt/project"
readonly SPARK_SUBMIT="/opt/bitnami/spark/bin/spark-submit"
readonly JOB_SCRIPT="${PROJECT_ROOT}/spark_apps/bronze/jobs/bronze_topic_job.py"

readonly PACKAGES=(
  "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
  "org.apache.spark:spark-avro_2.12:3.5.5"
  "org.apache.hadoop:hadoop-aws:3.3.4"
  "software.amazon.awssdk:bundle:2.20.160"
)

PACKAGE_LIST="$(
  IFS=,
  echo "${PACKAGES[*]}"
)"

echo "============================================================"
echo "Starting Bronze streaming job"
echo "Script: ${JOB_SCRIPT}"
echo "============================================================"

docker compose exec \
  spark-master \
  bash -lc "
    set -euo pipefail

    cd '${PROJECT_ROOT}'

    export PYTHONPATH='${PROJECT_ROOT}'
    export PYTHONDONTWRITEBYTECODE=1

    exec '${SPARK_SUBMIT}' \
      --master spark://spark-master:7077 \
      --conf spark.driver.host=spark-master \
      --conf spark.driver.bindAddress=0.0.0.0 \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      --conf spark.pyspark.python=/opt/bitnami/python/bin/python3 \
      --conf spark.pyspark.driver.python=/opt/bitnami/python/bin/python3 \
      --conf spark.executorEnv.PYTHONPATH='${PROJECT_ROOT}' \
      --conf spark.executorEnv.PYTHONDONTWRITEBYTECODE=1 \
      --packages '${PACKAGE_LIST}' \
      '${JOB_SCRIPT}'
  "
