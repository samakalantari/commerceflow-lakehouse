from typing import Dict, Optional, Tuple, TypedDict


class PartitionConfig(TypedDict):
    timestamp_field: str
    columns: Tuple[str, ...]


TOPIC_PARTITION_CONFIG: Dict[
    str,
    Optional[PartitionConfig],
] = {
    "transactional.categories": None,
    "transactional.products": {
        "timestamp_field": "ingested_at",
        "columns": (
            "year",
            "month",
            "day",
        ),
    },

    "transactional.users": {
        "timestamp_field": "signup_date",
        "columns": (
            "year",
            "month",
            "day",
        ),
    },

    "transactional.orders": {
        "timestamp_field": "timestamp",
        "columns": (
            "year",
            "month",
            "day",
        ),
    },

    "transactional.order_items": {
        "timestamp_field": "ingested_at",
        "columns": (
            "year",
            "month",
            "day",
        ),
    },

    "transactional.returns_refunds": {
        "timestamp_field": "ingested_at",
        "columns": (
            "year",
            "month",
            "day",
        ),
    },

    "transactional.product_price_history": {
        "timestamp_field": "valid_from",
        "columns": (
            "year",
            "month",
            "day",
        ),
    },

    "behavioral.events": {
        "timestamp_field": "timestamp",
        "columns": (
            "year",
            "month",
            "day",
        ),
    },
}


def get_partition_config(
    topic: str,
) -> Optional[PartitionConfig]:
    if topic not in TOPIC_PARTITION_CONFIG:
        raise ValueError(
            f"No partition configuration found for topic: {topic}"
        )

    return TOPIC_PARTITION_CONFIG[topic]


def get_partition_columns(
    topic: str,
) -> Tuple[str, ...]:
    config = get_partition_config(topic)

    if config is None:
        return ()

    return config["columns"]


def get_partition_timestamp_field(
    topic: str,
) -> Optional[str]:
    config = get_partition_config(topic)

    if config is None:
        return None

    return config["timestamp_field"]