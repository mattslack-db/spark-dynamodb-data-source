"""Tests for DynamoDB data source option validation."""

import sys
import types

import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql.types import StructType, StructField, StringType


@pytest.fixture(autouse=False)
def mock_databricks_sdk():
    """Inject a mock `databricks.service_credentials` module.

    The real package only exists on Databricks runtimes. The data source uses
    `databricks.service_credentials.getServiceCredentialsProvider(name)` from
    inside executor code (when a `TaskContext` is active) to obtain an
    auto-refreshing botocore Session.
    """
    mock_service_credentials = MagicMock()

    databricks_mod = types.ModuleType("databricks")
    service_creds_mod = types.ModuleType("databricks.service_credentials")
    service_creds_mod.getServiceCredentialsProvider = (
        mock_service_credentials.getServiceCredentialsProvider
    )
    databricks_mod.service_credentials = service_creds_mod

    with patch.dict(sys.modules, {
        "databricks": databricks_mod,
        "databricks.service_credentials": service_creds_mod,
    }):
        yield {"service_credentials": mock_service_credentials}


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


def test_create_table_requires_hash_key():
    """Test that create_table without hash_key raises ValueError."""
    from dynamodb_data_source import DynamoDbDataSource

    schema = StructType([StructField("id", StringType())])
    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
        "create_table": "true",
        # hash_key not provided
    }

    mock_session_class, _ = _mock_boto3_for_writer()

    ds = DynamoDbDataSource(options)

    with patch("boto3.Session", mock_session_class):
        with pytest.raises(ValueError, match="hash_key option is required"):
            ds.writer(schema, None)


def test_create_table_creates_when_not_exists():
    """Test that create_table creates a table when it doesn't exist."""
    import botocore.exceptions
    from dynamodb_data_source import DynamoDbDataSource

    schema = StructType([
        StructField("id", StringType()),
        StructField("sort_key", StringType()),
        StructField("data", StringType()),
    ])
    options = {
        "table_name": "new_table",
        "aws_region": "us-east-1",
        "create_table": "true",
        "hash_key": "id",
        "range_key": "sort_key",
    }

    mock_session_class = MagicMock()
    mock_session = MagicMock()
    mock_session_class.return_value = mock_session
    mock_dynamodb = MagicMock()
    mock_session.resource.return_value = mock_dynamodb

    # First call: table doesn't exist (for _create_table_if_not_exists)
    mock_missing_table = MagicMock()
    type(mock_missing_table).creation_date_time = property(
        lambda self: (_ for _ in ()).throw(
            botocore.exceptions.ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": ""}},
                "DescribeTable",
            )
        )
    )

    # Second call: table exists after creation (for _load_table_metadata)
    mock_created_table = MagicMock()
    mock_created_table.key_schema = [
        {"AttributeName": "id", "KeyType": "HASH"},
        {"AttributeName": "sort_key", "KeyType": "RANGE"},
    ]
    mock_created_table.load = MagicMock()

    # create_table returns the new table
    mock_new_table = MagicMock()
    mock_new_table.wait_until_exists = MagicMock()
    mock_dynamodb.create_table.return_value = mock_new_table

    # Table() is called twice from driver:
    # 1) create_table_if_not_exists check -> missing -> creates table
    # 2) load_table_metadata -> exists
    mock_dynamodb.Table.side_effect = [mock_missing_table, mock_created_table]

    ds = DynamoDbDataSource(options)

    with patch("boto3.Session", mock_session_class):
        writer = ds.writer(schema, None)

        # Verify create_table was called with correct args
        mock_dynamodb.create_table.assert_called_once()
        call_kwargs = mock_dynamodb.create_table.call_args[1]
        assert call_kwargs["TableName"] == "new_table"
        assert call_kwargs["BillingMode"] == "PAY_PER_REQUEST"
        assert len(call_kwargs["KeySchema"]) == 2
        assert call_kwargs["KeySchema"][0] == {"AttributeName": "id", "KeyType": "HASH"}
        assert call_kwargs["KeySchema"][1] == {"AttributeName": "sort_key", "KeyType": "RANGE"}
        mock_new_table.wait_until_exists.assert_called_once()


def test_create_table_skips_when_exists():
    """Test that create_table does nothing when table already exists."""
    from dynamodb_data_source import DynamoDbDataSource

    schema = StructType([StructField("id", StringType())])
    options = {
        "table_name": "existing_table",
        "aws_region": "us-east-1",
        "create_table": "true",
        "hash_key": "id",
    }

    mock_session_class, mock_table = _mock_boto3_for_writer()

    ds = DynamoDbDataSource(options)

    with patch("boto3.Session", mock_session_class):
        writer = ds.writer(schema, None)
        assert writer.table_name == "existing_table"
        # create_table should not have been called on the resource
        mock_session_class.return_value.resource.return_value.create_table.assert_not_called()


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


def test_writer_init_skips_aws_when_credential_name_set():
    """When `credential_name` is set, driver-side initialize() must not touch AWS.

    Driver-side `dbutils` is unreachable from the Spark Python data source
    callback process, so AWS calls are deferred to the first executor write.
    """
    from dynamodb_data_source import DynamoDbDataSource

    schema = StructType([StructField("id", StringType())])
    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
        "credential_name": "my-dynamo-credential",
    }

    mock_session_class = MagicMock()

    ds = DynamoDbDataSource(options)
    with patch("boto3.Session", mock_session_class):
        writer = ds.writer(schema, None)

    assert writer.credential_name == "my-dynamo-credential"
    assert writer._initialized is False
    mock_session_class.assert_not_called()


def test_writer_uses_executor_credentials_provider(mock_databricks_sdk):
    """Inside a TaskContext, write() must resolve via databricks.service_credentials.

    The botocore Session returned by `getServiceCredentialsProvider(name)` is
    passed as `boto3.Session(botocore_session=...)` so boto3 handles refresh.
    """
    from dynamodb_data_source.writer import DynamoDbBatchWriter

    mock_svc = mock_databricks_sdk["service_credentials"]
    sentinel_session = MagicMock(name="botocore_session")
    mock_svc.getServiceCredentialsProvider.return_value = sentinel_session

    schema = StructType([StructField("id", StringType())])
    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
        "credential_name": "my-dynamo-credential",
    }
    writer = DynamoDbBatchWriter(options, schema)
    # Skip _load_table_metadata for this unit test
    writer._initialized = True
    writer.key_schema = [{"AttributeName": "id", "KeyType": "HASH"}]
    writer.hash_key = "id"
    writer.range_key = None

    mock_session_class = MagicMock()
    mock_boto_session = MagicMock()
    mock_session_class.return_value = mock_boto_session
    mock_dynamodb = MagicMock()
    mock_boto_session.resource.return_value = mock_dynamodb
    mock_table = MagicMock()
    mock_dynamodb.Table.return_value = mock_table
    mock_table.batch_writer.return_value.__enter__.return_value = MagicMock()

    fake_task_ctx = MagicMock()
    with patch("boto3.Session", mock_session_class), \
         patch("pyspark.TaskContext.get", return_value=fake_task_ctx):
        writer.write(iter([]))

    mock_svc.getServiceCredentialsProvider.assert_called_with("my-dynamo-credential")
    mock_session_class.assert_called_with(
        region_name="us-east-1",
        botocore_session=sentinel_session,
    )


def test_reader_init_skips_aws_when_credential_name_set():
    """Reader __init__ with credential_name must not touch AWS or dbutils."""
    from dynamodb_data_source.reader import DynamoDbReader

    schema = StructType([StructField("id", StringType())])
    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
        "credential_name": "my-dynamo-credential",
    }
    reader = DynamoDbReader(options, schema)

    assert reader.credential_name == "my-dynamo-credential"
    assert reader.aws_access_key_id is None


def test_reader_uses_executor_credentials_provider(mock_databricks_sdk):
    """Inside a TaskContext, read() must resolve via databricks.service_credentials."""
    from dynamodb_data_source.reader import DynamoDbReader
    from dynamodb_data_source.partitioning import SegmentPartition

    mock_svc = mock_databricks_sdk["service_credentials"]
    sentinel_session = MagicMock(name="botocore_session")
    mock_svc.getServiceCredentialsProvider.return_value = sentinel_session

    schema = StructType([StructField("id", StringType())])
    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
        "credential_name": "my-dynamo-credential",
    }
    reader = DynamoDbReader(options, schema)

    mock_session_class = MagicMock()
    mock_boto_session = MagicMock()
    mock_session_class.return_value = mock_boto_session
    mock_dynamodb = MagicMock()
    mock_boto_session.resource.return_value = mock_dynamodb
    mock_table = MagicMock()
    mock_table.scan.return_value = {"Items": [{"id": "abc-123"}]}
    mock_dynamodb.Table.return_value = mock_table

    fake_task_ctx = MagicMock()
    with patch("boto3.Session", mock_session_class), \
         patch("pyspark.TaskContext.get", return_value=fake_task_ctx):
        list(reader.read(SegmentPartition(0, 1)))

    mock_svc.getServiceCredentialsProvider.assert_called_with("my-dynamo-credential")
    mock_session_class.assert_called_with(
        region_name="us-east-1",
        botocore_session=sentinel_session,
    )


def test_schema_with_credential_name_raises_when_dbutils_missing():
    """schema() with credential_name and no driver-side dbutils should raise a clear error."""
    from dynamodb_data_source import DynamoDbDataSource

    options = {
        "table_name": "test_table",
        "aws_region": "us-east-1",
        "credential_name": "my-dynamo-credential",
    }
    ds = DynamoDbDataSource(options)

    with pytest.raises(RuntimeError, match=r"explicit schema"):
        ds.schema()


def test_data_source_name():
    """Test that data source name is 'dynamodb'."""
    from dynamodb_data_source import DynamoDbDataSource

    assert DynamoDbDataSource.name() == "dynamodb"
