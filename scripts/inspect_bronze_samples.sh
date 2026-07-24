#!/usr/bin/env bash

set -euo pipefail

readonly PROJECT_ROOT="/opt/project"
readonly SPARK_SUBMIT="/opt/bitnami/spark/bin/spark-submit"
readonly INSPECT_SCRIPT="${PROJECT_ROOT}/spark_apps/bronze/jobs/inspect_bronze_samples.py"

readonly TOPIC="${1:-all}"
readonly LIMIT="${2:-5}"

readonly PACKAGES=(
  "org.apache.hadoop:hadoop-aws:3.3.4"
  "software.amazon.awssdk:bundle:2.20.160"
)

PACKAGE_LIST="$(
  IFS=,
  echo "${PACKAGES[*]}"
)"

if ! [[ "${LIMIT}" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: limit must be a positive integer."
  echo "Usage: $0 [topic|all] [limit]"
  exit 1
fi

echo "============================================================"
echo "Inspecting Bronze samples"
echo "Topic: ${TOPIC}"
echo "Limit: ${LIMIT}"
echo "============================================================"

docker compose exec -T \
  spark-master \
  bash -lc "
    set -euo pipefail

    cd '${PROJECT_ROOT}'

    export PYTHONPATH='${PROJECT_ROOT}'
    export PYTHONDONTWRITEBYTECODE=1

    '${SPARK_SUBMIT}' \
      --master spark://spark-master:7077 \
      --conf spark.driver.host=spark-master \
      --conf spark.driver.bindAddress=0.0.0.0 \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      --conf spark.pyspark.python=/opt/bitnami/python/bin/python3 \
      --conf spark.pyspark.driver.python=/opt/bitnami/python/bin/python3 \
      --conf spark.executorEnv.PYTHONPATH='${PROJECT_ROOT}' \
      --conf spark.executorEnv.PYTHONDONTWRITEBYTECODE=1 \
      --packages '${PACKAGE_LIST}' \
      '${INSPECT_SCRIPT}' \
      --topic '${TOPIC}' \
      --limit '${LIMIT}'
  "
