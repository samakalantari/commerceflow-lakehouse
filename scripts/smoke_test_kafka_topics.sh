#!/usr/bin/env bash

set -uo pipefail

readonly PROJECT_ROOT="/opt/project"
readonly SPARK_SUBMIT="/opt/bitnami/spark/bin/spark-submit"
readonly TEST_SCRIPT="${PROJECT_ROOT}/spark_apps/bronze/jobs/test_kafka_consumer.py"

readonly KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"

readonly MAX_OFFSETS_PER_TRIGGER="${KAFKA_MAX_OFFSETS_PER_TRIGGER:-50}"
readonly TEST_DURATION_SECONDS="${STREAM_TEST_DURATION_SECONDS:-25}"

topics=(
  "transactional.categories"
  "transactional.order_items"
  "transactional.orders"
  "transactional.product_price_history"
  "transactional.products"
  "transactional.returns_refunds"
  "transactional.users"
  "behavioral.events"
)

failed_topics=()

for topic in "${topics[@]}"; do
  echo
  echo "============================================================"
  echo "Kafka smoke test"
  echo "Topic: ${topic}"
  echo "============================================================"

  docker compose exec -T \
    -e KAFKA_TOPIC="${topic}" \
    -e KAFKA_MAX_OFFSETS_PER_TRIGGER="${MAX_OFFSETS_PER_TRIGGER}" \
    -e STREAM_TEST_DURATION_SECONDS="${TEST_DURATION_SECONDS}" \
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
        --packages '${KAFKA_PACKAGE}' \
        '${TEST_SCRIPT}'
    "

  status=$?

  if [[ "${status}" -eq 0 ]]; then
    echo "PASS: ${topic}"
  else
    echo "FAIL: ${topic}"
    failed_topics+=("${topic}")
  fi
done

echo
echo "============================================================"
echo "Kafka smoke test summary"
echo "============================================================"

if [[ "${#failed_topics[@]}" -eq 0 ]]; then
  echo "All Kafka topics passed."
  exit 0
fi

echo "Failed topics:"
printf " - %s\n" "${failed_topics[@]}"

exit 1
