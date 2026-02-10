"""Tests for DynamoDB reader logic."""

import pytest
from decimal import Decimal
from unittest.mock import MagicMock, patch
from pyspark.sql.types import StructType, StructField, StringType, LongType


def _mock_boto3_for_read():
    """Create boto3 mocks for reader read() calls."""
    mock_session_class = MagicMock()
    mock_session = MagicMock()
    mock_session_class.return_value = mock_session
    mock_dynamodb = MagicMock()
    mock_session.resource.return_value = mock_dynamodb
    return mock_session_class, mock_dynamodb


def test_reader_init_validates_options():
    """Test reader validates required options."""
    from dynamodb_data_source.reader import DynamoDbReader

    options = {}  # Missing required options
    schema = StructType([StructField("id", StringType())])

    with pytest.raises(ValueError, match="Missing required options"):
        DynamoDbReader(options, schema)


def test_reader_init_with_valid_options():
    """Test reader initializes with valid options (no DynamoDB connection)."""
    from dynamodb_data_source.reader import DynamoDbReader

    schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
    ])

    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
    }

    # No boto3 mock needed - reader __init__ does NOT connect to DynamoDB
    reader = DynamoDbReader(options, schema)

    assert reader.table_name == "test_table"
    assert reader.aws_region == "us-east-1"
    assert reader.schema == schema
    assert reader.columns == ["id", "name"]


def test_reader_uses_provided_schema():
    """Test reader uses user-provided schema."""
    from dynamodb_data_source.reader import DynamoDbReader

    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
    }
    user_schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType())
    ])

    reader = DynamoDbReader(options, user_schema)

    assert reader.schema == user_schema
    assert len(reader.schema.fields) == 2


def test_reader_partitions_single_segment():
    """Test reader creates one partition by default."""
    from dynamodb_data_source.reader import DynamoDbReader
    from dynamodb_data_source.partitioning import SegmentPartition

    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
    }
    schema = StructType([StructField("id", StringType())])

    reader = DynamoDbReader(options, schema)
    partitions = reader.partitions()

    assert len(partitions) == 1
    assert isinstance(partitions[0], SegmentPartition)
    assert partitions[0].segment == 0
    assert partitions[0].total_segments == 1


def test_reader_partitions_multiple_segments():
    """Test reader creates multiple partitions with total_segments."""
    from dynamodb_data_source.reader import DynamoDbReader
    from dynamodb_data_source.partitioning import SegmentPartition

    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
        "total_segments": "4",
    }
    schema = StructType([StructField("id", StringType())])

    reader = DynamoDbReader(options, schema)
    partitions = reader.partitions()

    assert len(partitions) == 4
    for i, p in enumerate(partitions):
        assert isinstance(p, SegmentPartition)
        assert p.segment == i
        assert p.total_segments == 4


def test_reader_read_returns_tuples():
    """Test read() returns tuples in schema column order."""
    from dynamodb_data_source.reader import DynamoDbReader
    from dynamodb_data_source.partitioning import SegmentPartition

    schema = StructType([
        StructField("age", LongType()),
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("score", LongType()),
    ])

    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
    }

    reader = DynamoDbReader(options, schema)

    # Mock for read (on executor)
    mock_read_table = MagicMock()
    mock_read_table.scan.return_value = {
        "Items": [
            {"id": "abc-123", "name": "Alice", "age": Decimal("30"), "score": Decimal("100")},
        ]
    }

    mock_session_class, mock_dynamodb = _mock_boto3_for_read()
    mock_dynamodb.Table.return_value = mock_read_table

    partition = SegmentPartition(0, 1)

    with patch("boto3.Session", mock_session_class):
        result = list(reader.read(partition))

    assert len(result) == 1
    assert isinstance(result[0], tuple)
    assert len(result[0]) == 4


def test_reader_read_converts_decimal():
    """Test read() converts Decimal values to int/float."""
    from dynamodb_data_source.reader import DynamoDbReader
    from dynamodb_data_source.partitioning import SegmentPartition

    user_schema = StructType([
        StructField("id", StringType()),
        StructField("count", LongType()),
    ])

    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
    }

    reader = DynamoDbReader(options, user_schema)

    # Mock for read
    mock_read_table = MagicMock()
    mock_read_table.scan.return_value = {
        "Items": [
            {"id": "abc-123", "count": Decimal("42")},
        ]
    }

    mock_session_class, mock_dynamodb = _mock_boto3_for_read()
    mock_dynamodb.Table.return_value = mock_read_table

    partition = SegmentPartition(0, 1)

    with patch("boto3.Session", mock_session_class):
        result = list(reader.read(partition))

    assert len(result) == 1
    # count should be converted from Decimal(42) to int(42)
    assert result[0][1] == 42
    assert isinstance(result[0][1], int)


def test_reader_read_paginates():
    """Test read() handles pagination via LastEvaluatedKey."""
    from dynamodb_data_source.reader import DynamoDbReader
    from dynamodb_data_source.partitioning import SegmentPartition

    user_schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
    ])

    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
    }

    reader = DynamoDbReader(options, user_schema)

    # Mock for read with pagination
    mock_read_table = MagicMock()
    mock_read_table.scan.side_effect = [
        {
            "Items": [{"id": "abc-1", "name": "Alice"}],
            "LastEvaluatedKey": {"id": "abc-1"},
        },
        {
            "Items": [{"id": "abc-2", "name": "Bob"}],
        },
    ]

    mock_session_class, mock_dynamodb = _mock_boto3_for_read()
    mock_dynamodb.Table.return_value = mock_read_table

    partition = SegmentPartition(0, 1)

    with patch("boto3.Session", mock_session_class):
        result = list(reader.read(partition))

    assert len(result) == 2
    assert mock_read_table.scan.call_count == 2


def test_reader_read_with_consistent_read():
    """Test read() passes ConsistentRead option."""
    from dynamodb_data_source.reader import DynamoDbReader
    from dynamodb_data_source.partitioning import SegmentPartition

    user_schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
    ])

    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
        "consistent_read": "true",
    }

    reader = DynamoDbReader(options, user_schema)

    mock_read_table = MagicMock()
    mock_read_table.scan.return_value = {"Items": []}

    mock_session_class, mock_dynamodb = _mock_boto3_for_read()
    mock_dynamodb.Table.return_value = mock_read_table

    partition = SegmentPartition(0, 1)

    with patch("boto3.Session", mock_session_class):
        list(reader.read(partition))

    scan_kwargs = mock_read_table.scan.call_args[1]
    assert scan_kwargs["ConsistentRead"] is True


def test_reader_read_with_parallel_segments():
    """Test read() passes Segment/TotalSegments for parallel scan."""
    from dynamodb_data_source.reader import DynamoDbReader
    from dynamodb_data_source.partitioning import SegmentPartition

    user_schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
    ])

    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
        "total_segments": "4",
    }

    reader = DynamoDbReader(options, user_schema)

    mock_read_table = MagicMock()
    mock_read_table.scan.return_value = {"Items": []}

    mock_session_class, mock_dynamodb = _mock_boto3_for_read()
    mock_dynamodb.Table.return_value = mock_read_table

    partition = SegmentPartition(2, 4)

    with patch("boto3.Session", mock_session_class):
        list(reader.read(partition))

    scan_kwargs = mock_read_table.scan.call_args[1]
    assert scan_kwargs["Segment"] == 2
    assert scan_kwargs["TotalSegments"] == 4
