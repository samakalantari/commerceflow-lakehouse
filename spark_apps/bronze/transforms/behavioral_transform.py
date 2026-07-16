from pyspark.sql import DataFrame
from pyspark.sql.functions import col, struct


BEHAVIORAL_TOPIC = "behavioral.events"

COMMON_EVENT_COLUMNS = (
    "timestamp",
    "user_id",
    "event_type",
    "device",
    "session_id",
)

EVENT_DATA_COLUMNS = (
    "product_id",
    "quantity",
    "cart_total_items",
    "cart_items",
    "cart_value",
    "shipping_method",
    "order_id",
    "fulfillment_speed",
    "url_path",
    "duration_sec",
    "http_status",
    "payment_type",
    "success",
    "error_code",
    "query",
    "results_count",
    "clicked_position",
    "rating",
    "text_length",
    "wishlist_name",
)


def group_behavioral_event_fields(
    df: DataFrame,
    topic: str,
) -> DataFrame:
    """
    Group event-specific behavioral fields into an event_data struct.

    Non-behavioral topics are returned unchanged.
    """
    if topic != BEHAVIORAL_TOPIC:
        return df

    missing_common_columns = [
        column
        for column in COMMON_EVENT_COLUMNS
        if column not in df.columns
    ]

    if missing_common_columns:
        missing = ", ".join(missing_common_columns)

        raise ValueError(
            "Missing required behavioral event columns: "
            f"{missing}"
        )

    existing_event_data_columns = [
        column
        for column in EVENT_DATA_COLUMNS
        if column in df.columns
    ]

    if not existing_event_data_columns:
        raise ValueError(
            "No behavioral event-specific columns were found."
        )

    metadata_columns = [
        column
        for column in df.columns
        if column.startswith("kafka_")
        or column == "ingested_at"
    ]

    return df.select(
        *[col(column) for column in metadata_columns],
        *[col(column) for column in COMMON_EVENT_COLUMNS],
        struct(
            *[
                col(column).alias(column)
                for column in existing_event_data_columns
            ]
        ).alias("event_data"),
    )