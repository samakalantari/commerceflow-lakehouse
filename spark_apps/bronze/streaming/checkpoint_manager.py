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

    safe_topic = normalize_name(topic)
    safe_query = normalize_name(query_name)
    safe_version = normalize_name(version)

    return (
        f"{base_path.rstrip('/')}/"
        f"{safe_query}/"
        f"{safe_topic}/"
        f"{safe_version}"
    )
