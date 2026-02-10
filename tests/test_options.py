"""Tests for DynamoDB data source option validation."""

import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql.types import StructType, StructField, StringType


def _mock_boto3_for_writer(key_schema=None):
    """Create boto3 mocks for writer option tests."""
    mock_session_class = MagicMock()
    mock_session = MagicMock()
    mock_session_class.return_value = mock_session
    mock_dynamodb = MagicMock()
    mock_session.resource.return_value = mock_dynamodb

    mock_table = MagicMock()
    mock_table.key_schema = key_schema or [{"AttributeName": "id", "KeyType": "HASH"}]
    mock_table.load = MagicMock()
    mock_dynamodb.Table.return_value = mock_table

    return mock_session_class, mock_table


def test_missing_required_option_table_name():
    """Test that missing table_name option raises ValueError."""
    from dynamodb_data_source import DynamoDbDataSource

    schema = StructType([StructField("id", StringType())])
    options = {"aws_region": "us-east-1"}

    ds = DynamoDbDataSource(options)

    with pytest.raises(ValueError, match="table_name"):
        ds.writer(schema, None)


def test_missing_required_option_aws_region():
    """Test that missing aws_region option raises ValueError."""
    from dynamodb_data_source import DynamoDbDataSource

    schema = StructType([StructField("id", StringType())])
    options = {"table_name": "test_table"}

    ds = DynamoDbDataSource(options)

    with pytest.raises(ValueError, match="aws_region"):
        ds.writer(schema, None)


def test_delete_flag_column_without_value():
    """Test that delete_flag_column without value raises ValueError."""
    from dynamodb_data_source import DynamoDbDataSource

    schema = StructType([StructField("id", StringType())])
    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
        "delete_flag_column": "is_deleted"
        # delete_flag_value not provided
    }

    mock_session_class, _ = _mock_boto3_for_writer()

    ds = DynamoDbDataSource(options)

    with patch("boto3.Session", mock_session_class):
        with pytest.raises(ValueError, match="must be specified together"):
            ds.writer(schema, None)


def test_delete_flag_value_without_column():
    """Test that delete_flag_value without column raises ValueError."""
    from dynamodb_data_source import DynamoDbDataSource

    schema = StructType([StructField("id", StringType())])
    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
        "delete_flag_value": "true"
        # delete_flag_column not provided
    }

    mock_session_class, _ = _mock_boto3_for_writer()

    ds = DynamoDbDataSource(options)

    with patch("boto3.Session", mock_session_class):
        with pytest.raises(ValueError, match="must be specified together"):
            ds.writer(schema, None)


def test_missing_key_column_in_schema():
    """Test that missing key column in schema raises ValueError."""
    from dynamodb_data_source import DynamoDbDataSource

    # Schema does NOT contain the key column "id"
    schema = StructType([StructField("name", StringType())])
    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
    }

    mock_session_class, _ = _mock_boto3_for_writer()

    ds = DynamoDbDataSource(options)

    with patch("boto3.Session", mock_session_class):
        with pytest.raises(ValueError, match="missing key columns"):
            ds.writer(schema, None)


def test_writer_with_all_options():
    """Test writer accepts all valid options."""
    from dynamodb_data_source import DynamoDbDataSource

    schema = StructType([StructField("id", StringType())])
    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
        "aws_access_key_id": "AKIATEST",
        "aws_secret_access_key": "secret123",
        "aws_session_token": "token123",
        "endpoint_url": "http://localhost:8000",
    }

    mock_session_class, _ = _mock_boto3_for_writer()

    ds = DynamoDbDataSource(options)

    with patch("boto3.Session", mock_session_class):
        writer = ds.writer(schema, None)
        assert writer.table_name == "test_table"
        assert writer.aws_region == "us-east-1"
        assert writer.aws_access_key_id == "AKIATEST"
        assert writer.aws_secret_access_key == "secret123"
        assert writer.aws_session_token == "token123"
        assert writer.endpoint_url == "http://localhost:8000"


def test_writer_default_options():
    """Test writer uses correct defaults for optional parameters."""
    from dynamodb_data_source import DynamoDbDataSource

    schema = StructType([StructField("id", StringType())])
    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
    }

    mock_session_class, _ = _mock_boto3_for_writer()

    ds = DynamoDbDataSource(options)

    with patch("boto3.Session", mock_session_class):
        writer = ds.writer(schema, None)
        assert writer.aws_access_key_id is None
        assert writer.aws_secret_access_key is None
        assert writer.aws_session_token is None
        assert writer.endpoint_url is None
        assert writer.delete_flag_column is None
        assert writer.delete_flag_value is None


def test_data_source_reader_method():
    """Test that reader() returns a DynamoDbBatchReader instance."""
    from dynamodb_data_source import DynamoDbDataSource, DynamoDbBatchReader

    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
    }
    schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType())
    ])

    ds = DynamoDbDataSource(options)

    # Reader __init__ does NOT connect to DynamoDB, so no mock needed
    reader = ds.reader(schema)

    assert isinstance(reader, DynamoDbBatchReader)
    assert reader.schema == schema


def test_data_source_schema_method(mock_dynamodb_table):
    """Test that schema() returns the derived schema."""
    from dynamodb_data_source import DynamoDbDataSource
    from pyspark.sql.types import StructType

    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
    }

    mock_session_class = MagicMock()
    mock_session = MagicMock()
    mock_session_class.return_value = mock_session
    mock_dynamodb = MagicMock()
    mock_session.resource.return_value = mock_dynamodb
    mock_dynamodb.Table.return_value = mock_dynamodb_table

    ds = DynamoDbDataSource(options)

    with patch("boto3.Session", mock_session_class):
        schema = ds.schema()

        assert isinstance(schema, StructType)
        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "name" in field_names


def test_data_source_name():
    """Test that data source name is 'dynamodb'."""
    from dynamodb_data_source import DynamoDbDataSource

    assert DynamoDbDataSource.name() == "dynamodb"
