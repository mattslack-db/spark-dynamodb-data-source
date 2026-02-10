"""
Integration tests against real AWS DynamoDB.

Requires environment variables:
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_SESSION_TOKEN  (optional, for temporary credentials)
    AWS_REGION         (optional, defaults to us-east-1)

To run:
    export AWS_ACCESS_KEY_ID=...
    export AWS_SECRET_ACCESS_KEY=...
    export AWS_SESSION_TOKEN=...
    poetry run pytest tests/test_aws_integration.py -v -m integration
"""

import os
import time
import uuid

import pytest

# Skip entire module if AWS credentials are not set
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"),
        reason="AWS credentials not set (need AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)",
    ),
]

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
TABLE_NAME = f"spark_dynamodb_test_{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def aws_table():
    """Create a temporary DynamoDB table for the test module, then delete it."""
    import boto3

    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)

    table = dynamodb.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()

    yield table

    # Cleanup
    table.delete()
    table.wait_until_not_exists()


@pytest.fixture(scope="module")
def aws_composite_table():
    """Create a temporary DynamoDB table with hash + range key."""
    import boto3

    composite_name = f"{TABLE_NAME}_composite"
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)

    table = dynamodb.create_table(
        TableName=composite_name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()

    yield table

    table.delete()
    table.wait_until_not_exists()


def _spark_options(table_name):
    """Return the common Spark .option() kwargs for AWS."""
    return {
        "table_name": table_name,
        "aws_region": AWS_REGION,
    }


def _clear_table(table):
    """Delete all items from a table."""
    key_attrs = [k["AttributeName"] for k in table.key_schema]
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={k: item[k] for k in key_attrs})


@pytest.fixture(scope="module", autouse=True)
def register_data_source(spark):
    """Register the DynamoDB data source once for the entire module."""
    from dynamodb_data_source import DynamoDbDataSource

    spark.dataSource.register(DynamoDbDataSource)


# ---------------------------------------------------------------------------
# Batch write tests
# ---------------------------------------------------------------------------


def test_batch_write_basic(spark, aws_table):
    """Write a DataFrame to DynamoDB and verify items land."""
    _clear_table(aws_table)

    data = [
        ("w-001", "Alice", 30, 100),
        ("w-002", "Bob", 25, 200),
        ("w-003", "Charlie", 35, 150),
    ]
    df = spark.createDataFrame(data, ["id", "name", "age", "score"])

    opts = _spark_options(TABLE_NAME)
    writer = df.write.format("dynamodb").mode("append")
    for k, v in opts.items():
        writer = writer.option(k, v)
    writer.save()

    # Allow propagation
    time.sleep(2)

    scan = aws_table.scan()
    items = scan.get("Items", [])
    assert len(items) == 3
    names = sorted(i["name"] for i in items)
    assert names == ["Alice", "Bob", "Charlie"]


def test_batch_write_overwrites_existing(spark, aws_table):
    """Writing the same key twice should overwrite the item."""
    _clear_table(aws_table)

    # First write
    df1 = spark.createDataFrame([("ow-001", "Original", 1, 1)], ["id", "name", "age", "score"])
    opts = _spark_options(TABLE_NAME)
    writer = df1.write.format("dynamodb").mode("append")
    for k, v in opts.items():
        writer = writer.option(k, v)
    writer.save()

    time.sleep(2)

    # Second write with same key, different value
    df2 = spark.createDataFrame([("ow-001", "Updated", 2, 2)], ["id", "name", "age", "score"])
    writer = df2.write.format("dynamodb").mode("append")
    for k, v in opts.items():
        writer = writer.option(k, v)
    writer.save()

    time.sleep(2)

    resp = aws_table.get_item(Key={"id": "ow-001"})
    assert resp["Item"]["name"] == "Updated"


def test_batch_write_with_delete_flag(spark, aws_table):
    """Delete flag in DataFrame should remove item from DynamoDB."""
    _clear_table(aws_table)

    # Pre-insert an item to be deleted
    aws_table.put_item(Item={"id": "del-001", "name": "Ghost", "age": 99, "score": 0})
    time.sleep(1)

    data = [
        ("del-001", "Ghost", 99, 0, True),   # delete
        ("del-002", "Alive", 42, 100, False),  # insert
    ]
    df = spark.createDataFrame(data, ["id", "name", "age", "score", "is_deleted"])

    opts = _spark_options(TABLE_NAME)
    writer = df.write.format("dynamodb").mode("append")
    for k, v in opts.items():
        writer = writer.option(k, v)
    writer.option("delete_flag_column", "is_deleted") \
          .option("delete_flag_value", "true") \
          .save()

    time.sleep(2)

    # Deleted item should be gone
    resp = aws_table.get_item(Key={"id": "del-001"})
    assert "Item" not in resp

    # Inserted item should exist
    resp = aws_table.get_item(Key={"id": "del-002"})
    assert resp["Item"]["name"] == "Alive"


def test_batch_write_composite_key(spark, aws_composite_table):
    """Write to a table with hash + range key."""
    _clear_table(aws_composite_table)

    data = [
        ("user-1", "2025-01-01", "event_a"),
        ("user-1", "2025-01-02", "event_b"),
        ("user-2", "2025-01-01", "event_c"),
    ]
    df = spark.createDataFrame(data, ["pk", "sk", "payload"])

    composite_name = f"{TABLE_NAME}_composite"
    opts = _spark_options(composite_name)
    writer = df.write.format("dynamodb").mode("append")
    for k, v in opts.items():
        writer = writer.option(k, v)
    writer.save()

    time.sleep(2)

    scan = aws_composite_table.scan()
    items = scan.get("Items", [])
    assert len(items) == 3


# ---------------------------------------------------------------------------
# Batch read tests
# ---------------------------------------------------------------------------


def test_batch_read_basic(spark, aws_table):
    """Read all items from a DynamoDB table into a DataFrame."""
    _clear_table(aws_table)

    # Seed data directly
    for i in range(5):
        aws_table.put_item(Item={"id": f"r-{i:03d}", "name": f"User{i}", "age": 20 + i, "score": i * 10})
    time.sleep(1)

    opts = _spark_options(TABLE_NAME)
    reader = spark.read.format("dynamodb")
    for k, v in opts.items():
        reader = reader.option(k, v)
    df = reader.load()

    assert df.count() == 5
    assert "id" in df.columns
    assert "name" in df.columns
    assert "age" in df.columns
    assert "score" in df.columns


def test_batch_read_with_schema_projection(spark, aws_table):
    """Read with explicit schema should only return projected columns."""
    from pyspark.sql.types import StructType, StructField, StringType

    _clear_table(aws_table)

    aws_table.put_item(Item={"id": "sp-001", "name": "Alice", "age": 30, "score": 100})
    aws_table.put_item(Item={"id": "sp-002", "name": "Bob", "age": 25, "score": 200})
    time.sleep(1)

    schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
    ])

    opts = _spark_options(TABLE_NAME)
    reader = spark.read.format("dynamodb").schema(schema)
    for k, v in opts.items():
        reader = reader.option(k, v)
    df = reader.load()

    assert set(df.columns) == {"id", "name"}
    rows = df.collect()
    assert len(rows) == 2


def test_batch_read_parallel_segments(spark, aws_table):
    """Read with multiple segments produces the same total count."""
    _clear_table(aws_table)

    for i in range(20):
        aws_table.put_item(Item={"id": f"seg-{i:03d}", "name": f"User{i}"})
    time.sleep(1)

    opts = _spark_options(TABLE_NAME)
    reader = spark.read.format("dynamodb").option("total_segments", "4")
    for k, v in opts.items():
        reader = reader.option(k, v)
    df = reader.load()

    assert df.count() == 20


def test_batch_read_empty_table(spark, aws_table):
    """Read from an empty table should return zero rows."""
    _clear_table(aws_table)
    time.sleep(1)

    opts = _spark_options(TABLE_NAME)
    reader = spark.read.format("dynamodb")
    for k, v in opts.items():
        reader = reader.option(k, v)
    df = reader.load()

    assert df.count() == 0


def test_batch_read_composite_key(spark, aws_composite_table):
    """Read from a table with hash + range key."""
    _clear_table(aws_composite_table)

    aws_composite_table.put_item(Item={"pk": "u1", "sk": "2025-01", "data": "a"})
    aws_composite_table.put_item(Item={"pk": "u1", "sk": "2025-02", "data": "b"})
    aws_composite_table.put_item(Item={"pk": "u2", "sk": "2025-01", "data": "c"})
    time.sleep(1)

    composite_name = f"{TABLE_NAME}_composite"
    opts = _spark_options(composite_name)
    reader = spark.read.format("dynamodb")
    for k, v in opts.items():
        reader = reader.option(k, v)
    df = reader.load()

    assert df.count() == 3
    assert "pk" in df.columns
    assert "sk" in df.columns


# ---------------------------------------------------------------------------
# Write-then-read round-trip tests
# ---------------------------------------------------------------------------


def test_write_then_read_roundtrip(spark, aws_table):
    """Write via Spark, read back via Spark, verify data matches."""
    _clear_table(aws_table)

    data = [
        ("rt-001", "Alice", 30, 100),
        ("rt-002", "Bob", 25, 200),
    ]
    df_write = spark.createDataFrame(data, ["id", "name", "age", "score"])

    opts = _spark_options(TABLE_NAME)
    writer = df_write.write.format("dynamodb").mode("append")
    for k, v in opts.items():
        writer = writer.option(k, v)
    writer.save()

    time.sleep(2)

    reader = spark.read.format("dynamodb")
    for k, v in opts.items():
        reader = reader.option(k, v)
    df_read = reader.load()

    rows = {r["id"]: r for r in df_read.collect()}
    assert len(rows) == 2
    assert rows["rt-001"]["name"] == "Alice"
    assert rows["rt-002"]["name"] == "Bob"


# ---------------------------------------------------------------------------
# Streaming write test
# ---------------------------------------------------------------------------


def test_stream_write_with_rate_source(spark, aws_table):
    """Write a micro-batch stream to DynamoDB using the rate source."""
    import pyspark.sql.functions as F

    _clear_table(aws_table)

    # The rate source generates (timestamp, value) pairs
    stream_df = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", 5)
        .option("numPartitions", 1)
        .load()
        .withColumn("id", F.concat(F.lit("stream-"), F.col("value").cast("string")))
        .withColumn("name", F.lit("streamed"))
        .select("id", "name")
    )

    opts = _spark_options(TABLE_NAME)
    query = stream_df.writeStream.format("dynamodb").outputMode("append")
    for k, v in opts.items():
        query = query.option(k, v)
    query = query.option("checkpointLocation", f"/tmp/spark_ddb_test_{uuid.uuid4().hex[:8]}")
    running = query.start()

    try:
        # Let a few micro-batches run
        time.sleep(10)
    finally:
        running.stop()

    # Verify some items were written
    scan = aws_table.scan()
    items = [i for i in scan.get("Items", []) if i["id"].startswith("stream-")]
    assert len(items) > 0, "Expected at least one streamed item in DynamoDB"
