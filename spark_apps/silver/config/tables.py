from spark_apps.silver.config.iceberg import (
    ICEBERG_CATALOG_NAME,
)


SILVER_NAMESPACE = "silver"

SILVER_DATABASE = (
    f"{ICEBERG_CATALOG_NAME}.{SILVER_NAMESPACE}"
)


# ---------------------------------------------------------
# Bronze transactional topics
# ---------------------------------------------------------

TOPIC_CATEGORIES = "transactional.categories"
TOPIC_ORDER_ITEMS = "transactional.order_items"
TOPIC_ORDERS = "transactional.orders"
TOPIC_PRODUCT_PRICE_HISTORY = (
    "transactional.product_price_history"
)
TOPIC_PRODUCTS = "transactional.products"
TOPIC_RETURNS_REFUNDS = (
    "transactional.returns_refunds"
)
TOPIC_USERS = "transactional.users"


TRANSACTIONAL_TOPICS = (
    TOPIC_CATEGORIES,
    TOPIC_ORDER_ITEMS,
    TOPIC_ORDERS,
    TOPIC_PRODUCT_PRICE_HISTORY,
    TOPIC_PRODUCTS,
    TOPIC_RETURNS_REFUNDS,
    TOPIC_USERS,
)


# ---------------------------------------------------------
# Silver dimensions
# ---------------------------------------------------------

DIM_DATE = (
    f"{SILVER_DATABASE}.dim_date"
)

DIM_USER = (
    f"{SILVER_DATABASE}.dim_user"
)

DIM_PRODUCT = (
    f"{SILVER_DATABASE}.dim_product"
)


# ---------------------------------------------------------
# Silver facts
# ---------------------------------------------------------

FACT_ORDER = (
    f"{SILVER_DATABASE}.fact_order"
)

FACT_ORDER_ITEM = (
    f"{SILVER_DATABASE}.fact_order_item"
)


# ---------------------------------------------------------
# Data quality / quarantine
# ---------------------------------------------------------

QUARANTINE_NAMESPACE = "silver_quarantine"

QUARANTINE_DATABASE = (
    f"{ICEBERG_CATALOG_NAME}."
    f"{QUARANTINE_NAMESPACE}"
)

INVALID_USERS = (
    f"{QUARANTINE_DATABASE}.invalid_users"
)

INVALID_PRODUCTS = (
    f"{QUARANTINE_DATABASE}.invalid_products"
)

INVALID_ORDERS = (
    f"{QUARANTINE_DATABASE}.invalid_orders"
)

INVALID_ORDER_ITEMS = (
    f"{QUARANTINE_DATABASE}.invalid_order_items"
)
