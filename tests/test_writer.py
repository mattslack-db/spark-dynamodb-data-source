"""Tests for DynamoDB writer execution logic."""

import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    BooleanType,
    DoubleType,
)


def _mock_boto3(mock_dynamodb_table):
    """Helper to create standard boto3 mocks for writer tests."""
    mock_session_class = MagicMock()
    mock_session = MagicMock()
    mock_session_class.return_value = mock_session
    mock_dynamodb_resource = MagicMock()
    mock_session.resource.return_value = mock_dynamodb_resource
    mock_dynamodb_resource.Table.return_value = mock_dynamodb_table

    # Setup batch_writer context manager
    mock_batch = MagicMock()
    mock_dynamodb_table.batch_writer.return_value.__enter__ = MagicMock(return_value=mock_batch)
    mock_dynamodb_table.batch_writer.return_value.__exit__ = MagicMock(return_value=False)

    return mock_session_class, mock_batch


def test_write_basic_insert(basic_options, sample_schema, mock_dynamodb_table):
    """Test basic write operation with inserts."""
    from dynamodb_data_source import DynamoDbDataSource

    mock_session_class, mock_batch = _mock_boto3(mock_dynamodb_table)

    with patch("boto3.Session", mock_session_class):
        ds = DynamoDbDataSource(basic_options)
        writer = ds.writer(sample_schema, None)

        rows = [
            Row(id="abc-123", name="Alice", age=30, score=100),
            Row(id="abc-456", name="Bob", age=25, score=200)
        ]

        result = writer.write(iter(rows))

        # Verify put_item was called for each row
        assert mock_batch.put_item.call_count == 2
        assert result is not None


def test_write_with_delete_flag(basic_options, sample_schema, mock_dynamodb_table):
    """Test write operation with delete flag."""
    from dynamodb_data_source import DynamoDbDataSource

    # Add delete flag to schema
    schema_with_flag = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("score", LongType(), True),
        StructField("is_deleted", BooleanType(), True)
    ])

    options = {**basic_options, "delete_flag_column": "is_deleted", "delete_flag_value": "true"}

    mock_session_class, mock_batch = _mock_boto3(mock_dynamodb_table)

    with patch("boto3.Session", mock_session_class):
        ds = DynamoDbDataSource(options)
        writer = ds.writer(schema_with_flag, None)

        # One insert, one delete
        rows = [
            Row(id="abc-123", name="Alice", age=30, score=100, is_deleted=False),
            Row(id="abc-456", name="Bob", age=25, score=200, is_deleted=True)
        ]

        writer.write(iter(rows))

        # Verify both put_item and delete_item were called
        assert mock_batch.put_item.call_count == 1
        assert mock_batch.delete_item.call_count == 1

        # Verify delete was called with correct key
        delete_call = mock_batch.delete_item.call_args
        assert delete_call[1]["Key"]["id"] == "abc-456"


def test_write_float_to_decimal_conversion(basic_options, mock_dynamodb_table):
    """Test that float values are converted to Decimal for DynamoDB."""
    from dynamodb_data_source import DynamoDbDataSource
    from decimal import Decimal

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("price", StringType(), True),  # Spark schema type doesn't matter for dict conversion
    ])

    mock_session_class, mock_batch = _mock_boto3(mock_dynamodb_table)

    with patch("boto3.Session", mock_session_class):
        ds = DynamoDbDataSource(basic_options)
        writer = ds.writer(schema, None)

        rows = [Row(id="abc-123", price=19.99)]

        writer.write(iter(rows))

        # Verify put_item was called
        assert mock_batch.put_item.call_count == 1
        put_call = mock_batch.put_item.call_args
        item = put_call[1]["Item"]

        # Float should have been converted to Decimal
        assert isinstance(item["price"], Decimal)


def test_write_eav_typed_value_columns(basic_options, mock_dynamodb_table):
    """EAV rows using type-specific value columns convert correctly on write.

    A single Spark column cannot hold a different type per row, so a realistic
    EAV layout uses type-specific value columns (value_num, value_str,
    value_bool) with only one populated per row. The schema here matches the
    values it carries — exercising the real conversion write() performs on the
    typed Rows Spark hands it: float -> Decimal, others unchanged.
    """
    from dynamodb_data_source import DynamoDbDataSource
    from decimal import Decimal

    mock_dynamodb_table.key_schema = [
        {"AttributeName": "persona_id", "KeyType": "HASH"},
        {"AttributeName": "attribute_name", "KeyType": "RANGE"},
    ]

    schema = StructType([
        StructField("persona_id", StringType(), False),
        StructField("attribute_name", StringType(), False),
        StructField("value_num", DoubleType(), True),
        StructField("value_str", StringType(), True),
        StructField("value_bool", BooleanType(), True),
    ])

    mock_session_class, mock_batch = _mock_boto3(mock_dynamodb_table)

    with patch("boto3.Session", mock_session_class):
        ds = DynamoDbDataSource(basic_options)
        writer = ds.writer(schema, None)

        rows = [
            Row(persona_id="p1", attribute_name="score", value_num=9.5, value_str=None, value_bool=None),
            Row(persona_id="p1", attribute_name="name", value_num=None, value_str="Alice", value_bool=None),
            Row(persona_id="p1", attribute_name="active", value_num=None, value_str=None, value_bool=True),
        ]

        writer.write(iter(rows))

        items = {c[1]["Item"]["attribute_name"]: c[1]["Item"] for c in mock_batch.put_item.call_args_list}

        assert isinstance(items["score"]["value_num"], Decimal)
        assert float(items["score"]["value_num"]) == 9.5
        assert items["name"]["value_str"] == "Alice"
        assert items["active"]["value_bool"] is True


def test_write_delete_converts_float_key_to_decimal(basic_options, mock_dynamodb_table):
    """A numeric (float) key on the DELETE path is converted to Decimal.

    The PUT path converts item values; the DELETE key must convert too, or
    boto3 rejects the float. Regression test for the put/delete asymmetry.
    """
    from dynamodb_data_source import DynamoDbDataSource
    from decimal import Decimal

    # Numeric hash key.
    mock_dynamodb_table.key_schema = [{"AttributeName": "sensor_id", "KeyType": "HASH"}]

    schema = StructType([
        StructField("sensor_id", DoubleType(), False),
        StructField("is_deleted", BooleanType(), True),
    ])

    options = {**basic_options, "delete_flag_column": "is_deleted", "delete_flag_value": "true"}

    mock_session_class, mock_batch = _mock_boto3(mock_dynamodb_table)

    with patch("boto3.Session", mock_session_class):
        ds = DynamoDbDataSource(options)
        writer = ds.writer(schema, None)

        writer.write(iter([Row(sensor_id=3.14, is_deleted=True)]))

        delete_call = mock_batch.delete_item.call_args
        key_value = delete_call[1]["Key"]["sensor_id"]
        assert isinstance(key_value, Decimal)
        assert float(key_value) == 3.14


def test_invalid_rate_limit_non_numeric_raises(basic_options, sample_schema, mock_dynamodb_table):
    """A non-numeric rate limit fails fast with a clear message, not a cryptic cast error."""
    from dynamodb_data_source import DynamoDbDataSource

    mock_session_class, _ = _mock_boto3(mock_dynamodb_table)
    options = {**basic_options, "max_writes_per_second": "fast"}

    with patch("boto3.Session", mock_session_class):
        ds = DynamoDbDataSource(options)
        with pytest.raises(ValueError, match="max_writes_per_second must be a number"):
            ds.writer(sample_schema, None)


def test_write_null_key_raises_error(basic_options, sample_schema, mock_dynamodb_table):
    """Test that null key values raise ValueError."""
    from dynamodb_data_source import DynamoDbDataSource

    mock_session_class, mock_batch = _mock_boto3(mock_dynamodb_table)

    with patch("boto3.Session", mock_session_class):
        ds = DynamoDbDataSource(basic_options)
        writer = ds.writer(sample_schema, None)

        rows = [Row(id=None, name="Alice", age=30, score=100)]

        with pytest.raises(ValueError, match="Key column 'id' cannot be null"):
            writer.write(iter(rows))


def test_write_delete_null_key_raises_error(basic_options, sample_schema, mock_dynamodb_table):
    """Test that null key values raise ValueError on delete."""
    from dynamodb_data_source import DynamoDbDataSource

    schema_with_flag = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("score", LongType(), True),
        StructField("is_deleted", BooleanType(), True)
    ])

    options = {**basic_options, "delete_flag_column": "is_deleted", "delete_flag_value": "true"}

    mock_session_class, mock_batch = _mock_boto3(mock_dynamodb_table)

    with patch("boto3.Session", mock_session_class):
        ds = DynamoDbDataSource(options)
        writer = ds.writer(schema_with_flag, None)

        rows = [Row(id=None, name="Bob", age=25, score=200, is_deleted=True)]

        with pytest.raises(ValueError, match="Key column 'id' cannot be null"):
            writer.write(iter(rows))


def test_write_with_range_key(basic_options, sample_schema):
    """Test write with composite key (hash + range)."""
    from dynamodb_data_source import DynamoDbDataSource

    # Mock table with composite key
    mock_table = MagicMock()
    mock_table.key_schema = [
        {"AttributeName": "id", "KeyType": "HASH"},
        {"AttributeName": "name", "KeyType": "RANGE"},
    ]
    mock_table.load = MagicMock()

    mock_session_class, mock_batch = _mock_boto3(mock_table)

    options = {**basic_options, "delete_flag_column": "is_deleted", "delete_flag_value": "true"}

    schema_with_flag = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("score", LongType(), True),
        StructField("is_deleted", BooleanType(), True)
    ])

    with patch("boto3.Session", mock_session_class):
        ds = DynamoDbDataSource(options)
        writer = ds.writer(schema_with_flag, None)

        rows = [Row(id="abc-456", name="Bob", age=25, score=200, is_deleted=True)]

        writer.write(iter(rows))

        # Delete should include both hash and range key
        delete_call = mock_batch.delete_item.call_args
        assert delete_call[1]["Key"]["id"] == "abc-456"
        assert delete_call[1]["Key"]["name"] == "Bob"


def test_write_rate_limit_acquires_per_row(basic_options, sample_schema, mock_dynamodb_table):
    """When max_writes_per_second is set, the writer paces one permit per row."""
    from dynamodb_data_source import DynamoDbDataSource

    mock_session_class, mock_batch = _mock_boto3(mock_dynamodb_table)
    options = {**basic_options, "max_writes_per_second": "100"}

    with patch("boto3.Session", mock_session_class), \
         patch("dynamodb_data_source.writer.TokenBucketRateLimiter") as limiter_cls:
        limiter = limiter_cls.return_value
        ds = DynamoDbDataSource(options)
        writer = ds.writer(sample_schema, None)

        rows = [
            Row(id="a", name="A", age=1, score=1),
            Row(id="b", name="B", age=2, score=2),
        ]
        writer.write(iter(rows))

        limiter_cls.assert_called_once_with(100.0)
        assert limiter.acquire.call_count == 2


def test_write_no_rate_limit_by_default(basic_options, sample_schema, mock_dynamodb_table):
    """Without the option, no limiter is created (backward compatible)."""
    from dynamodb_data_source import DynamoDbDataSource

    mock_session_class, mock_batch = _mock_boto3(mock_dynamodb_table)

    with patch("boto3.Session", mock_session_class), \
         patch("dynamodb_data_source.writer.TokenBucketRateLimiter") as limiter_cls:
        ds = DynamoDbDataSource(basic_options)
        writer = ds.writer(sample_schema, None)
        writer.write(iter([Row(id="a", name="A", age=1, score=1)]))

        limiter_cls.assert_not_called()


def test_invalid_rate_limit_raises(basic_options, sample_schema, mock_dynamodb_table):
    """A non-positive rate limit fails fast with a clear message."""
    from dynamodb_data_source import DynamoDbDataSource

    mock_session_class, _ = _mock_boto3(mock_dynamodb_table)
    options = {**basic_options, "max_writes_per_second": "0"}

    with patch("boto3.Session", mock_session_class):
        ds = DynamoDbDataSource(options)
        with pytest.raises(ValueError, match="max_writes_per_second must be positive"):
            ds.writer(sample_schema, None)


def test_write_dedups_duplicate_keys_within_batch(basic_options, sample_schema, mock_dynamodb_table):
    """The writer must de-duplicate primary keys within a single batch.

    boto3's BatchWriteItem rejects a batch containing two operations on the same
    key ('Provided list of item keys contains duplicates'), which fails the
    whole batch. Passing overwrite_by_pkeys to batch_writer makes boto3 keep
    only the last op per key per flush (last-write-wins). Regression test for a
    partition/microbatch that repeats keys (e.g. a CDC diff or rate stream).
    """
    from dynamodb_data_source import DynamoDbDataSource

    mock_session_class, mock_batch = _mock_boto3(mock_dynamodb_table)

    with patch("boto3.Session", mock_session_class):
        ds = DynamoDbDataSource(basic_options)
        writer = ds.writer(sample_schema, None)
        writer.write(iter([Row(id="a", name="A", age=1, score=1)]))

        # batch_writer must dedup by the table's primary key attribute(s).
        mock_dynamodb_table.batch_writer.assert_called_once_with(overwrite_by_pkeys=["id"])


def test_write_dedups_by_composite_key(basic_options, mock_dynamodb_table):
    """Dedup must use the FULL primary key (hash + range), not just the hash."""
    from dynamodb_data_source import DynamoDbDataSource

    mock_dynamodb_table.key_schema = [
        {"AttributeName": "pk", "KeyType": "HASH"},
        {"AttributeName": "sk", "KeyType": "RANGE"},
    ]
    schema = StructType([
        StructField("pk", StringType(), False),
        StructField("sk", StringType(), False),
        StructField("payload", StringType(), True),
    ])

    mock_session_class, mock_batch = _mock_boto3(mock_dynamodb_table)

    with patch("boto3.Session", mock_session_class):
        ds = DynamoDbDataSource(basic_options)
        writer = ds.writer(schema, None)
        writer.write(iter([Row(pk="u1", sk="a", payload="x")]))

        mock_dynamodb_table.batch_writer.assert_called_once_with(overwrite_by_pkeys=["pk", "sk"])


def test_write_returns_commit_message(basic_options, sample_schema, mock_dynamodb_table):
    """Test that write returns a WriterCommitMessage."""
    from dynamodb_data_source import DynamoDbDataSource
    from pyspark.sql.datasource import WriterCommitMessage

    mock_session_class, mock_batch = _mock_boto3(mock_dynamodb_table)

    with patch("boto3.Session", mock_session_class):
        ds = DynamoDbDataSource(basic_options)
        writer = ds.writer(sample_schema, None)

        rows = [Row(id="abc-123", name="Alice", age=30, score=100)]

        result = writer.write(iter(rows))

        assert isinstance(result, WriterCommitMessage)
