"""Tests for DynamoDB streaming writer."""

from unittest.mock import patch, MagicMock
from pyspark.sql import Row


def _mock_boto3(mock_dynamodb_table):
    """Helper to create standard boto3 mocks."""
    mock_session_class = MagicMock()
    mock_session = MagicMock()
    mock_session_class.return_value = mock_session
    mock_dynamodb_resource = MagicMock()
    mock_session.resource.return_value = mock_dynamodb_resource
    mock_dynamodb_resource.Table.return_value = mock_dynamodb_table

    mock_batch = MagicMock()
    mock_dynamodb_table.batch_writer.return_value.__enter__ = MagicMock(return_value=mock_batch)
    mock_dynamodb_table.batch_writer.return_value.__exit__ = MagicMock(return_value=False)

    return mock_session_class, mock_batch


def test_stream_writer_commit(basic_options, sample_schema, mock_dynamodb_table):
    """Test stream writer commit method."""
    from dynamodb_data_source import DynamoDbDataSource

    mock_session_class, _ = _mock_boto3(mock_dynamodb_table)

    with patch("boto3.Session", mock_session_class):
        ds = DynamoDbDataSource(basic_options)
        writer = ds.streamWriter(sample_schema, False)

        # Commit should not raise error
        messages = [MagicMock()]
        writer.commit(messages, 0)


def test_stream_writer_abort(basic_options, sample_schema, mock_dynamodb_table):
    """Test stream writer abort method."""
    from dynamodb_data_source import DynamoDbDataSource

    mock_session_class, _ = _mock_boto3(mock_dynamodb_table)

    with patch("boto3.Session", mock_session_class):
        ds = DynamoDbDataSource(basic_options)
        writer = ds.streamWriter(sample_schema, False)

        # Abort should not raise error
        messages = [MagicMock()]
        writer.abort(messages, 0)


def test_stream_writer_write(basic_options, sample_schema, mock_dynamodb_table):
    """Test that stream writer can write data."""
    from dynamodb_data_source import DynamoDbDataSource

    mock_session_class, mock_batch = _mock_boto3(mock_dynamodb_table)

    with patch("boto3.Session", mock_session_class):
        ds = DynamoDbDataSource(basic_options)
        writer = ds.streamWriter(sample_schema, False)

        rows = [
            Row(id="abc-123", name="Alice", age=30, score=100)
        ]

        result = writer.write(iter(rows))

        assert mock_batch.put_item.call_count == 1
        assert result is not None
