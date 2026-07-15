from unittest.mock import MagicMock, patch

import pytest
import requests

from spark_apps.bronze.decoders.avro_decoder import (
    SCHEMA_REGISTRY_URL,
    _fetch_schema,
    decode,
)


@pytest.fixture(autouse=True)
def clear_schema_cache():
    _fetch_schema.cache_clear()

    yield

    _fetch_schema.cache_clear()


@patch(
    "spark_apps.bronze.decoders.avro_decoder.requests.get"
)
def test_fetch_schema_requests_latest_topic_value_schema(
    mock_get,
):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "schema": '{"type":"record","name":"Order"}'
    }

    mock_get.return_value = response

    result = _fetch_schema(
        "transactional.orders"
    )

    mock_get.assert_called_once_with(
        (
            f"{SCHEMA_REGISTRY_URL}/subjects/"
            "transactional.orders-value/"
            "versions/latest"
        ),
        timeout=10,
    )

    assert result == (
        '{"type":"record","name":"Order"}'
    )


@patch(
    "spark_apps.bronze.decoders.avro_decoder.requests.get"
)
def test_fetch_schema_returns_none_for_missing_subject(
    mock_get,
):
    response = MagicMock()
    response.status_code = 404

    mock_get.return_value = response

    result = _fetch_schema(
        "transactional.returns_refunds"
    )

    assert result is None

    response.raise_for_status.assert_not_called()


@patch(
    "spark_apps.bronze.decoders.avro_decoder.requests.get"
)
def test_fetch_schema_raises_for_http_error(
    mock_get,
):
    response = MagicMock()
    response.status_code = 500
    response.raise_for_status.side_effect = (
        requests.HTTPError(
            "Schema Registry error"
        )
    )

    mock_get.return_value = response

    with pytest.raises(
        requests.HTTPError,
        match="Schema Registry error",
    ):
        _fetch_schema(
            "transactional.orders"
        )

    response.raise_for_status.assert_called_once_with()


@patch(
    "spark_apps.bronze.decoders.avro_decoder.requests.get"
)
def test_fetch_schema_propagates_connection_error(
    mock_get,
):
    mock_get.side_effect = requests.ConnectionError(
        "Schema Registry unavailable"
    )

    with pytest.raises(
        requests.ConnectionError,
        match="Schema Registry unavailable",
    ):
        _fetch_schema(
            "transactional.orders"
        )


@patch(
    "spark_apps.bronze.decoders.avro_decoder.requests.get"
)
def test_fetch_schema_propagates_timeout(
    mock_get,
):
    mock_get.side_effect = requests.Timeout(
        "Schema Registry timed out"
    )

    with pytest.raises(
        requests.Timeout,
        match="Schema Registry timed out",
    ):
        _fetch_schema(
            "transactional.orders"
        )


@patch(
    "spark_apps.bronze.decoders.avro_decoder.requests.get"
)
def test_fetch_schema_caches_result_for_same_topic(
    mock_get,
):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "schema": '{"type":"record","name":"Order"}'
    }

    mock_get.return_value = response

    first_result = _fetch_schema(
        "transactional.orders"
    )

    second_result = _fetch_schema(
        "transactional.orders"
    )

    assert first_result == second_result
    mock_get.assert_called_once()


@patch(
    "spark_apps.bronze.decoders.avro_decoder.requests.get"
)
def test_fetch_schema_does_not_share_cache_between_topics(
    mock_get,
):
    first_response = MagicMock()
    first_response.status_code = 200
    first_response.json.return_value = {
        "schema": '{"name":"Order"}'
    }

    second_response = MagicMock()
    second_response.status_code = 200
    second_response.json.return_value = {
        "schema": '{"name":"Product"}'
    }

    mock_get.side_effect = [
        first_response,
        second_response,
    ]

    order_schema = _fetch_schema(
        "transactional.orders"
    )

    product_schema = _fetch_schema(
        "transactional.products"
    )

    assert order_schema == '{"name":"Order"}'
    assert product_schema == '{"name":"Product"}'
    assert mock_get.call_count == 2

@patch(
    "spark_apps.bronze.decoders.avro_decoder._fetch_schema"
)
def test_decode_returns_none_when_schema_is_not_registered(
    mock_fetch_schema,
):
    df = MagicMock()
    df.columns = ["raw_value"]

    mock_fetch_schema.return_value = None

    result = decode(
        df=df,
        topic="transactional.returns_refunds",
        payload_column="raw_value",
    )

    assert result is None

    mock_fetch_schema.assert_called_once_with(
        "transactional.returns_refunds"
    )

    df.withColumn.assert_not_called()

@patch(
    "spark_apps.bronze.decoders.avro_decoder.from_avro"
)
@patch(
    "spark_apps.bronze.decoders.avro_decoder.expr"
)
@patch(
    "spark_apps.bronze.decoders.avro_decoder._fetch_schema"
)
def test_decode_removes_confluent_header(
    mock_fetch_schema,
    mock_expr,
    mock_from_avro,
):
    df = MagicMock()
    decoded_df = MagicMock()

    df.columns = [
        "kafka_key",
        "raw_value",
        "kafka_topic",
    ]

    mock_fetch_schema.return_value = (
        '{"type":"record","name":"Order","fields":[]}'
    )

    payload_expression = MagicMock(
        name="payload_expression"
    )
    decoded_expression = MagicMock(
        name="decoded_expression"
    )

    mock_expr.return_value = payload_expression
    mock_from_avro.return_value = decoded_expression
    df.withColumn.return_value = decoded_df

    decoded_field = MagicMock()
    decoded_field.dataType.fieldNames.return_value = []

    decoded_df.schema.__getitem__.return_value = decoded_field
    decoded_df.select.return_value = MagicMock()

    decode(
        df=df,
        topic="transactional.orders",
        payload_column="raw_value",
    )

    mock_expr.assert_called_once_with(
        "substring(raw_value, 6, length(raw_value) - 5)"
    )

    mock_from_avro.assert_called_once_with(
        payload_expression,
        (
            '{"type":"record","name":"Order",'
            '"fields":[]}'
        ),
    )

    df.withColumn.assert_called_once_with(
        "_decoded",
        decoded_expression,
    )


@patch(
    "spark_apps.bronze.decoders.avro_decoder.col"
)
@patch(
    "spark_apps.bronze.decoders.avro_decoder.from_avro"
)
@patch(
    "spark_apps.bronze.decoders.avro_decoder.expr"
)
@patch(
    "spark_apps.bronze.decoders.avro_decoder._fetch_schema"
)
def test_decode_flattens_decoded_fields(
    mock_fetch_schema,
    mock_expr,
    mock_from_avro,
    mock_col,
):
    df = MagicMock()
    decoded_df = MagicMock()
    result_df = MagicMock()

    df.columns = [
        "kafka_key",
        "raw_value",
        "kafka_topic",
        "kafka_offset",
    ]

    mock_fetch_schema.return_value = (
        '{"type":"record","name":"Order"}'
    )

    mock_expr.return_value = MagicMock()
    mock_from_avro.return_value = MagicMock()
    df.withColumn.return_value = decoded_df

    decoded_field = MagicMock()
    decoded_field.dataType.fieldNames.return_value = [
        "order_id",
        "status",
        "total_amount",
    ]

    decoded_df.schema.__getitem__.return_value = decoded_field
    decoded_df.select.return_value = result_df

    mocked_columns = {}

    def build_mock_column(column_name):
        column = MagicMock(
            name=f"column_{column_name}"
        )
        column.alias.return_value = column
        mocked_columns[column_name] = column
        return column

    mock_col.side_effect = build_mock_column

    result = decode(
        df=df,
        topic="transactional.orders",
        payload_column="raw_value",
    )

    mock_col.assert_any_call(
        "_decoded.order_id"
    )
    mock_col.assert_any_call(
        "_decoded.status"
    )
    mock_col.assert_any_call(
        "_decoded.total_amount"
    )

    mocked_columns[
        "_decoded.order_id"
    ].alias.assert_called_once_with(
        "order_id"
    )

    mocked_columns[
        "_decoded.status"
    ].alias.assert_called_once_with(
        "status"
    )

    mocked_columns[
        "_decoded.total_amount"
    ].alias.assert_called_once_with(
        "total_amount"
    )

    decoded_df.select.assert_called_once()

    assert result is result_df


@patch(
    "spark_apps.bronze.decoders.avro_decoder.col"
)
@patch(
    "spark_apps.bronze.decoders.avro_decoder.from_avro"
)
@patch(
    "spark_apps.bronze.decoders.avro_decoder.expr"
)
@patch(
    "spark_apps.bronze.decoders.avro_decoder._fetch_schema"
)
def test_decode_preserves_metadata_columns_and_removes_payload(
    mock_fetch_schema,
    mock_expr,
    mock_from_avro,
    mock_col,
):
    df = MagicMock()
    decoded_df = MagicMock()

    df.columns = [
        "kafka_key",
        "raw_value",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "ingested_at",
    ]

    mock_fetch_schema.return_value = (
        '{"type":"record","name":"Order"}'
    )

    mock_expr.return_value = MagicMock()
    mock_from_avro.return_value = MagicMock()
    df.withColumn.return_value = decoded_df

    decoded_field = MagicMock()
    decoded_field.dataType.fieldNames.return_value = [
        "order_id",
    ]

    decoded_df.schema.__getitem__.return_value = decoded_field

    selected_arguments = []

    def capture_select(*args):
        selected_arguments.extend(args)
        return MagicMock()

    decoded_df.select.side_effect = capture_select

    decoded_column = MagicMock()
    decoded_column.alias.return_value = decoded_column
    mock_col.return_value = decoded_column

    decode(
        df=df,
        topic="transactional.orders",
        payload_column="raw_value",
    )

    assert "kafka_key" in selected_arguments
    assert "kafka_topic" in selected_arguments
    assert "kafka_partition" in selected_arguments
    assert "kafka_offset" in selected_arguments
    assert "kafka_timestamp" in selected_arguments
    assert "ingested_at" in selected_arguments

    assert "raw_value" not in selected_arguments


@patch(
    "spark_apps.bronze.decoders.avro_decoder.from_avro"
)
@patch(
    "spark_apps.bronze.decoders.avro_decoder.expr"
)
@patch(
    "spark_apps.bronze.decoders.avro_decoder._fetch_schema"
)
def test_decode_uses_default_value_payload_column(
    mock_fetch_schema,
    mock_expr,
    mock_from_avro,
):
    df = MagicMock()
    decoded_df = MagicMock()

    df.columns = [
        "key",
        "value",
        "topic",
    ]

    mock_fetch_schema.return_value = (
        '{"type":"record","name":"Event"}'
    )

    mock_expr.return_value = MagicMock()
    mock_from_avro.return_value = MagicMock()
    df.withColumn.return_value = decoded_df

    decoded_field = MagicMock()
    decoded_field.dataType.fieldNames.return_value = []

    decoded_df.schema.__getitem__.return_value = decoded_field
    decoded_df.select.return_value = MagicMock()

    decode(
        df=df,
        topic="behavioral.events",
    )

    mock_expr.assert_called_once_with(
        "substring(value, 6, length(value) - 5)"
    )


@patch(
    "spark_apps.bronze.decoders.avro_decoder._fetch_schema"
)
def test_decode_propagates_schema_registry_error(
    mock_fetch_schema,
):
    df = MagicMock()
    df.columns = ["raw_value"]

    mock_fetch_schema.side_effect = (
        requests.ConnectionError(
            "Schema Registry unavailable"
        )
    )

    with pytest.raises(
        requests.ConnectionError,
        match="Schema Registry unavailable",
    ):
        decode(
            df=df,
            topic="transactional.orders",
            payload_column="raw_value",
        )

@patch(
    "spark_apps.bronze.decoders.avro_decoder._fetch_schema"
)
def test_decode_rejects_missing_payload_column(
    mock_fetch_schema,
):
    df = MagicMock()

    df.columns = [
        "kafka_key",
        "kafka_topic",
    ]

    with pytest.raises(
        ValueError,
        match="Payload column 'raw_value' not found",
    ):
        decode(
            df=df,
            topic="transactional.orders",
            payload_column="raw_value",
        )

    mock_fetch_schema.assert_not_called()