#!/usr/bin/env bash
# scripts/run_bronze_job.sh

set -euo pipefail

CONTAINER="group4-data-platform-spark-master-1"
PROJECT_ROOT="/opt/spark-apps/bronze"
SCRIPT="$PROJECT_ROOT/jobs/bronze_topic_job.py"
PACKAGES="org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.spark:spark-avro_2.12:3.5.5"

docker exec -it "$CONTAINER" \
  env PYTHONPATH="$PROJECT_ROOT" \
  spark-submit \
  --packages "$PACKAGES" \
  "$SCRIPT"
