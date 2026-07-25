from spark_apps.silver.config.tables import (
    INVALID_ORDER_ITEMS,
    INVALID_ORDERS,
    INVALID_RETURNS_REFUNDS,
    INVALID_USERS,
)
from spark_apps.silver.jobs import (
    load_dim_user,
    load_fact_order,
    load_fact_order_item,
    load_fact_return_refund,
)


def test_transactional_loaders_use_expected_quarantine_tables():
    assert load_dim_user.INVALID_USERS == INVALID_USERS
    assert load_fact_order.INVALID_ORDERS == INVALID_ORDERS
    assert load_fact_order_item.INVALID_ORDER_ITEMS == INVALID_ORDER_ITEMS
    assert load_fact_return_refund.INVALID_RETURNS_REFUNDS == INVALID_RETURNS_REFUNDS
