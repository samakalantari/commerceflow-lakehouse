#spark-apps/bronze/sinks/minio_sink.py
import os
from pyspark.sql import DataFrame

def write_bronze_stream(df: DataFrame, topic: str, checkpoint_base: str):
    path = _topic_to_path(topic)
    checkpoint = f"{checkpoint_base}/{topic.replace('.', '/')}"

    return (
        df.writeStream
        .format("parquet")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .partitionBy("event_date")
        .outputMode("append")
        .start()
    )

def _topic_to_path(topic: str) -> str:
    base = os.environ["BRONZE_KAFKA_BASE_PATH"].rstrip("/")
    return f"{base}/{topic.replace('.', '/')}"


