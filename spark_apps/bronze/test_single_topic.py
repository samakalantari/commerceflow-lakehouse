import os
os.environ["TEST_TOPIC"] = "transactional.categories"

from pyspark.sql import SparkSession
from decoders.avro_decoder import decode
from config.topic_metadata import PARTITION_TS_FIELD

#spark = SparkSession.builder.appName("bronze-test").getOrCreate()
#spark.sparkContext.setLogLevel("WARN")

import os

spark = SparkSession.builder \
    .appName("bronze-test") \
    .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"]) \
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"]) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"]) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()



topic = "transactional.categories"

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "185.255.90.14:9092")
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()
)

decoded = decode(raw, topic)
assert decoded is not None, f"Schema not found for {topic}"

from pyspark.sql.functions import col, year, month, dayofmonth, to_timestamp
ts_field = PARTITION_TS_FIELD[topic]
result = decoded.withColumn("year", year(col("timestamp"))) \
                .withColumn("month", month(col("timestamp"))) \
                .withColumn("day", dayofmonth(col("timestamp")))


query = (
    result.writeStream
    .format("parquet")
    .option("path", "s3a://commerceflow-lakehouse/bronze/transactional/categories/")
    .option("checkpointLocation", "/tmp/checkpoint_test_categories/")
    .partitionBy("year", "month", "day")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
print("✅ Done. Check MinIO.")
