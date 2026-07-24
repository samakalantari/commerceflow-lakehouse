from functools import lru_cache

import requests
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr

SCHEMA_REGISTRY_URL = "http://185.255.90.14:8081"


@lru_cache(maxsize=32)
def _fetch_schema(topic: str) -> str | None:
    """
    Fetch the latest Avro schema for a topic's value from Schema Registry.
    Returns None if the subject doesn't exist yet (e.g. topic with no
    messages produced so far, like transactional.returns_refunds).
    """
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{topic}-value/versions/latest"
    resp = requests.get(url, timeout=10)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()["schema"]


def decode(df, topic: str, payload_column: str = "value"):
    """
    Decode Confluent-wire-format Avro binary data from a Kafka `value` column.
    Wire format: [magic_byte(1) | schema_id(4) | avro_payload].

    Returns None if no schema is registered for this topic yet - callers
    must check for this and skip starting a streaming query for it
    (limit()/filter() shortcuts on the raw streaming DataFrame are not
    a safe way to produce an "empty" result here).
    """
    if payload_column not in df.columns:
        raise ValueError(
            f"Payload column '{payload_column}' "
            "not found in DataFrame. "
            f"Available columns: {df.columns}"
        )

    avro_schema = _fetch_schema(topic)

    if avro_schema is None:
        return None

    # strip 5-byte Confluent header (1 magic byte + 4-byte schema id)
    payload = expr(f"substring({payload_column}, 6, length({payload_column}) - 5)")

    decoded = df.withColumn("_decoded", from_avro(payload, avro_schema))
    fields = decoded.schema["_decoded"].dataType.fieldNames()

    return decoded.select(
        *[c for c in df.columns if c != payload_column],
        *[col(f"_decoded.{f}").alias(f) for f in fields],
    )
