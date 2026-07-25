from spark_apps.silver.config.tables import INVALID_PRODUCTS
from spark_apps.silver.jobs import load_dim_product


def test_load_dim_product_uses_product_quarantine_table():
    assert load_dim_product.INVALID_PRODUCTS == INVALID_PRODUCTS
