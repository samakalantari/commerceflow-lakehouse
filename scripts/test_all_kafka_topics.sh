#!/usr/bin/env bash

set -u

topics=(
  transactional.categories
  transactional.order_items
  transactional.orders
  transactional.product_price_history
  transactional.products
  transactional.returns_refunds
  transactional.users
  behavioral.events
)

failed_topics=()

for topic in "${topics[@]}"; do
  echo
  echo "========================================"
  echo "Testing topic: $topic"
  echo "========================================"

  docker compose exec -T \
    -e KAFKA_TOPIC="$topic" \
    -e KAFKA_MAX_OFFSETS_PER_TRIGGER=50 \
    -e STREAM_TEST_DURATION_SECONDS=25 \
    spark-master bash -lc '
      cd /opt/spark-apps || exit 1

      export PYTHONPATH=/opt/spark-apps
      export PYTHONDONTWRITEBYTECODE=1

      /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.driver.host=spark-master \
        --conf spark.driver.bindAddress=0.0.0.0 \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --conf spark.pyspark.python=/opt/bitnami/python/bin/python3 \
        --conf spark.pyspark.driver.python=/opt/bitnami/python/bin/python3 \
        --conf spark.executorEnv.PYTHONPATH=/opt/spark-apps \
        --conf spark.executorEnv.PYTHONDONTWRITEBYTECODE=1 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
        bronze/jobs/test_kafka_consumer.py
    '

  status=$?

  if [ "$status" -eq 0 ]; then
    echo "PASS: $topic"
  else
    echo "FAIL: $topic"
    failed_topics+=("$topic")
  fi
done

echo
echo "========================================"

if [ "${#failed_topics[@]}" -eq 0 ]; then
  echo "All Kafka topics passed."
  exit 0
fi

echo "Failed topics:"
printf " - %s\n" "${failed_topics[@]}"
exit 1
