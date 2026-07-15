BUSINESS_TOPICS = (
    "transactional.categories",
    "transactional.order_items",
    "transactional.orders",
    "transactional.product_price_history",
    "transactional.products",
    "transactional.returns_refunds",
    "transactional.users",
    "behavioral.events",
)

def validate_topic(topic: str) -> str:
    if topic not in BUSINESS_TOPICS:
        allowed = ", ".join(BUSINESS_TOPICS)
        raise ValueError(
            f"Unsupported Kafka topic: {topic}. "
            f"Allowed topics: {allowed}"
        )

    return topic
