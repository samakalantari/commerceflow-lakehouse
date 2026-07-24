import re


def normalize_name(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_-]+", "_", value)
    return normalized.strip("_").lower()


def build_checkpoint_path(
    base_path: str,
    topic: str,
    query_name: str,
    version: str = "v1",
) -> str:
    if not base_path:
        raise ValueError("Checkpoint base path must not be empty")

    if not topic:
        raise ValueError("Kafka topic must not be empty")

    if not query_name:
        raise ValueError("Query name must not be empty")

    if not version:
        raise ValueError("Checkpoint version must not be empty")

    safe_topic = normalize_name(topic)
    safe_query = normalize_name(query_name)
    safe_version = normalize_name(version)

    if not safe_topic:
        raise ValueError("Kafka topic must contain valid characters")

    if not safe_query:
        raise ValueError("Query name must contain valid characters")

    if not safe_version:
        raise ValueError("Checkpoint version must contain valid characters")

    return f"{base_path.rstrip('/')}/{safe_query}/{safe_topic}/{safe_version}"
