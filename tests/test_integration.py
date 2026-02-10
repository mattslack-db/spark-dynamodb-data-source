"""
Integration tests requiring a running DynamoDB Local instance.

To run these tests:
1. Start DynamoDB Local: cd tests && docker-compose up -d
2. Wait for ready: curl http://localhost:8000
3. Run tests: poetry run pytest tests/test_integration.py -v -m integration
4. Stop DynamoDB Local: cd tests && docker-compose down
"""

import pytest
import time


@pytest.fixture(scope="module")
def dynamodb_setup():
    """Setup test table in DynamoDB Local."""
    import boto3

    dynamodb = boto3.resource(
        "dynamodb",
        region_name="us-east-1",
        endpoint_url="http://localhost:8000",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

    # Create table
    try:
        table = dynamodb.create_table(
            TableName="test_table",
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        table = dynamodb.Table("test_table")

    # Clear table
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"id": item["id"]})

    yield table

    # Cleanup
    table.delete()


@pytest.fixture(scope="module", autouse=True)
def register_data_source(spark):
    """Register the DynamoDB data source once for the entire module."""
    from dynamodb_data_source import DynamoDbDataSource

    spark.dataSource.register(DynamoDbDataSource)


@pytest.mark.integration
def test_write_and_read_integration(spark, dynamodb_setup):
    """Test full write and read cycle with real DynamoDB Local."""

    # Create test DataFrame
    data = [
        ("id-001", "Alice", 30, 100),
        ("id-002", "Bob", 25, 200),
        ("id-003", "Charlie", 35, 150),
    ]
    df = spark.createDataFrame(data, ["id", "name", "age", "score"])

    # Write to DynamoDB
    df.write.format("dynamodb") \
        .option("table_name", "test_table") \
        .option("aws_region", "us-east-1") \
        .option("endpoint_url", "http://localhost:8000") \
        .option("aws_access_key_id", "test") \
        .option("aws_secret_access_key", "test") \
        .save()

    time.sleep(1)

    # Verify data was written
    scan = dynamodb_setup.scan()
    items = scan.get("Items", [])
    assert len(items) == 3


@pytest.mark.integration
def test_write_with_delete_flag_integration(spark, dynamodb_setup):
    """Test write with delete flag using real DynamoDB Local."""

    # Insert initial data
    dynamodb_setup.put_item(Item={"id": "id-delete", "name": "ToDelete", "age": 40, "score": 300})

    # Create DataFrame with delete flag
    data = [("id-delete", "ToDelete", 40, 300, True)]
    df = spark.createDataFrame(data, ["id", "name", "age", "score", "is_deleted"])

    # Write with delete flag
    df.write.format("dynamodb") \
        .option("table_name", "test_table") \
        .option("aws_region", "us-east-1") \
        .option("endpoint_url", "http://localhost:8000") \
        .option("aws_access_key_id", "test") \
        .option("aws_secret_access_key", "test") \
        .option("delete_flag_column", "is_deleted") \
        .option("delete_flag_value", "true") \
        .save()

    time.sleep(1)

    # Verify row was deleted
    response = dynamodb_setup.get_item(Key={"id": "id-delete"})
    assert "Item" not in response


@pytest.mark.integration
@pytest.mark.manual
def test_write_streaming_integration():
    """
    Manual test for streaming writes.

    NOTE: This test is skipped because you cannot use createDataFrame with writeStream.
    createDataFrame creates a batch DataFrame, not a streaming DataFrame.

    To test streaming writes:
    1. Set up a real streaming source (Kafka, socket, rate source, etc.)
    2. Use that streaming source with dynamodb sink
    3. Verify data flows correctly
    """
    pytest.skip(
        "Manual test - requires actual streaming source. Cannot use createDataFrame with writeStream."
    )


@pytest.mark.integration
def test_read_basic_integration(spark, dynamodb_setup):
    """Test basic read operation from DynamoDB Local."""
    # Insert test data
    test_data = [
        {"id": "read-001", "name": "Alice", "age": 30, "score": 100},
        {"id": "read-002", "name": "Bob", "age": 25, "score": 200},
        {"id": "read-003", "name": "Charlie", "age": 35, "score": 150},
    ]
    for item in test_data:
        dynamodb_setup.put_item(Item=item)

    time.sleep(1)

    # Read from DynamoDB
    df = spark.read.format("dynamodb") \
        .option("table_name", "test_table") \
        .option("aws_region", "us-east-1") \
        .option("endpoint_url", "http://localhost:8000") \
        .option("aws_access_key_id", "test") \
        .option("aws_secret_access_key", "test") \
        .load()

    # Verify data was read
    count = df.count()
    assert count >= 3

    assert "id" in df.columns
    assert "name" in df.columns


@pytest.mark.integration
def test_read_with_schema_projection_integration(spark, dynamodb_setup):
    """Test reading with explicit schema (column projection)."""
    from pyspark.sql.types import StructType, StructField, StringType

    # Insert test data
    dynamodb_setup.put_item(Item={"id": "proj-001", "name": "Alice", "age": 30, "score": 100})
    dynamodb_setup.put_item(Item={"id": "proj-002", "name": "Bob", "age": 25, "score": 200})

    time.sleep(1)

    schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType())
    ])

    df = spark.read.format("dynamodb") \
        .schema(schema) \
        .option("table_name", "test_table") \
        .option("aws_region", "us-east-1") \
        .option("endpoint_url", "http://localhost:8000") \
        .option("aws_access_key_id", "test") \
        .option("aws_secret_access_key", "test") \
        .load()

    # Verify only selected columns
    assert len(df.columns) == 2
    assert "id" in df.columns
    assert "name" in df.columns

    rows = df.collect()
    assert len(rows) >= 2
