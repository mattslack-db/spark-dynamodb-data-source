"""Tests for DynamoDB writer execution logic."""

import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType


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
