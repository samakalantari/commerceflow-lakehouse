import pytest

from spark_apps.bronze.config.topics import (
    BUSINESS_TOPICS,
    validate_topic,
)


EXPECTED_TOPICS = {
    "transactional.categories",
    "transactional.order_items",
    "transactional.orders",
    "transactional.product_price_history",
    "transactional.products",
    "transactional.returns_refunds",
    "transactional.users",
    "behavioral.events",
}


def test_business_topics_contains_expected_topics():
    assert set(BUSINESS_TOPICS) == EXPECTED_TOPICS


def test_business_topics_does_not_contain_duplicates():
    assert len(BUSINESS_TOPICS) == len(set(BUSINESS_TOPICS))


@pytest.mark.parametrize("topic", BUSINESS_TOPICS)
def test_validate_topic_returns_valid_topic(topic):
    assert validate_topic(topic) == topic


def test_validate_topic_raises_error_for_unsupported_topic():
    with pytest.raises(ValueError, match="Unsupported Kafka topic"):
        validate_topic("transactional.unknown")


def test_validate_topic_error_contains_allowed_topics():
    with pytest.raises(ValueError) as exc_info:
        validate_topic("invalid.topic")

    error_message = str(exc_info.value)

    for topic in BUSINESS_TOPICS:
        assert topic in error_message