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


@pytest.fixture(scope="module")
def aws_ttl_table():
    """Create a temporary DynamoDB table with TTL enabled."""
    import boto3

    ttl_table_name = f"{TABLE_NAME}_ttl"
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    client = boto3.client("dynamodb", region_name=AWS_REGION)

    table = dynamodb.create_table(
        TableName=ttl_table_name,
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()

    # Enable TTL on the "expire_at" attribute
    client.update_time_to_live(
        TableName=ttl_table_name,
        TimeToLiveSpecification={
            "Enabled": True,
            "AttributeName": "expire_at",
        },
    )

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


def test_batch_write_delete_flag_composite_key(spark, aws_composite_table):
    """Delete flag should work correctly with hash + range key tables."""
    _clear_table(aws_composite_table)

    # Pre-insert items via boto3
    aws_composite_table.put_item(Item={"pk": "u1", "sk": "2025-01", "payload": "old_a"})
    aws_composite_table.put_item(Item={"pk": "u1", "sk": "2025-02", "payload": "old_b"})
    aws_composite_table.put_item(Item={"pk": "u2", "sk": "2025-01", "payload": "old_c"})
    time.sleep(1)

    # Delete one item, upsert another, insert a new one
    data = [
        ("u1", "2025-01", "old_a", "yes"),    # delete
        ("u1", "2025-02", "updated_b", "no"),  # upsert (keep)
        ("u3", "2025-03", "new_d", "no"),      # insert
    ]
    df = spark.createDataFrame(data, ["pk", "sk", "payload", "del_flag"])

    composite_name = f"{TABLE_NAME}_composite"
    opts = _spark_options(composite_name)
    writer = df.write.format("dynamodb").mode("append")
    for k, v in opts.items():
        writer = writer.option(k, v)
    writer.option("delete_flag_column", "del_flag") \
          .option("delete_flag_value", "yes") \
          .save()

    time.sleep(2)

    # Deleted item should be gone
    resp = aws_composite_table.get_item(Key={"pk": "u1", "sk": "2025-01"})
    assert "Item" not in resp

    # Upserted item should have updated payload
    resp = aws_composite_table.get_item(Key={"pk": "u1", "sk": "2025-02"})
    assert resp["Item"]["payload"] == "updated_b"

    # New item should exist
    resp = aws_composite_table.get_item(Key={"pk": "u3", "sk": "2025-03"})
    assert resp["Item"]["payload"] == "new_d"

    # Untouched item should still exist
    resp = aws_composite_table.get_item(Key={"pk": "u2", "sk": "2025-01"})
    assert resp["Item"]["payload"] == "old_c"


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


def test_batch_write_with_ttl(spark, aws_ttl_table):
    """Write items with a TTL column and verify the epoch value is stored."""
    _clear_table(aws_ttl_table)

    ttl_table_name = f"{TABLE_NAME}_ttl"

    # expire_at is a Unix epoch timestamp (int seconds)
    # One item set far in the future, one set in the past
    future_ts = int(time.time()) + 86400 * 365  # ~1 year from now
    past_ts = int(time.time()) - 3600            # 1 hour ago

    data = [
        ("ttl-001", "Permanent", future_ts),
        ("ttl-002", "Expired", past_ts),
    ]
    df = spark.createDataFrame(data, ["id", "name", "expire_at"])

    opts = _spark_options(ttl_table_name)
    writer = df.write.format("dynamodb").mode("append")
    for k, v in opts.items():
        writer = writer.option(k, v)
    writer.save()

    time.sleep(2)

    # Verify items were written with the TTL attribute
    resp = aws_ttl_table.get_item(Key={"id": "ttl-001"})
    assert "Item" in resp
    assert resp["Item"]["expire_at"] == future_ts

    resp = aws_ttl_table.get_item(Key={"id": "ttl-002"})
    assert "Item" in resp
    assert resp["Item"]["expire_at"] == past_ts

    # Verify TTL is enabled on the table
    import boto3

    client = boto3.client("dynamodb", region_name=AWS_REGION)
    ttl_desc = client.describe_time_to_live(TableName=ttl_table_name)
    ttl_status = ttl_desc["TimeToLiveDescription"]["TimeToLiveStatus"]
    assert ttl_status in ("ENABLED", "ENABLING")
    assert ttl_desc["TimeToLiveDescription"]["AttributeName"] == "expire_at"


def test_batch_write_and_read_with_ttl(spark, aws_ttl_table):
    """Write items with TTL via Spark, read back, verify the TTL column round-trips."""
    _clear_table(aws_ttl_table)

    ttl_table_name = f"{TABLE_NAME}_ttl"
    future_ts = int(time.time()) + 86400 * 30  # 30 days from now

    data = [
        ("ttlrt-001", "Alice", future_ts),
        ("ttlrt-002", "Bob", future_ts + 100),
    ]
    df_write = spark.createDataFrame(data, ["id", "name", "expire_at"])

    opts = _spark_options(ttl_table_name)
    writer = df_write.write.format("dynamodb").mode("append")
    for k, v in opts.items():
        writer = writer.option(k, v)
    writer.save()

    time.sleep(2)

    # Read back via Spark
    reader = spark.read.format("dynamodb")
    for k, v in opts.items():
        reader = reader.option(k, v)
    df_read = reader.load()

    assert "expire_at" in df_read.columns
    rows = {r["id"]: r for r in df_read.collect()}
    assert len(rows) == 2
    assert rows["ttlrt-001"]["expire_at"] == future_ts
    assert rows["ttlrt-002"]["expire_at"] == future_ts + 100


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


def test_write_and_read_at_scale(spark, aws_table):
    """Write 10k records via Spark, read back, verify count and data integrity."""
    _clear_table(aws_table)

    # Generate 10,000 rows and repartition for parallel writes
    data = [(f"scale-{i:05d}", f"User_{i}", i % 100, i * 10) for i in range(10_000)]
    df_write = spark.createDataFrame(data, ["id", "name", "age", "score"]).repartition(4)

    opts = _spark_options(TABLE_NAME)
    writer = df_write.write.format("dynamodb").mode("append")
    for k, v in opts.items():
        writer = writer.option(k, v)
    writer.save()

    time.sleep(5)

    # Read back with parallel segments for speed
    reader = spark.read.format("dynamodb").option("total_segments", "4")
    for k, v in opts.items():
        reader = reader.option(k, v)
    df_read = reader.load()

    assert df_read.count() == 10_000

    # Spot-check a few known records
    rows = {r["id"]: r for r in df_read.filter(df_read.id.isin("scale-00000", "scale-05000", "scale-09999")).collect()}
    assert rows["scale-00000"]["name"] == "User_0"
    assert rows["scale-05000"]["name"] == "User_5000"
    assert rows["scale-09999"]["name"] == "User_9999"


# ---------------------------------------------------------------------------
# Create table tests
# ---------------------------------------------------------------------------


def test_create_table_hash_key_only(spark):
    """create_table option should auto-create a table with a hash key and write data."""
    import boto3

    auto_table_name = f"{TABLE_NAME}_autocreate"
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)

    # Ensure table doesn't exist before the test
    try:
        t = dynamodb.Table(auto_table_name)
        t.delete()
        t.wait_until_not_exists()
    except Exception:
        pass

    try:
        data = [("ac-001", "Alice", 30), ("ac-002", "Bob", 25)]
        df = spark.createDataFrame(data, ["id", "name", "age"])

        opts = _spark_options(auto_table_name)
        writer = df.write.format("dynamodb").mode("append")
        for k, v in opts.items():
            writer = writer.option(k, v)
        writer.option("create_table", "true") \
              .option("hash_key", "id") \
              .save()

        time.sleep(2)

        # Verify table was created and data was written
        table = dynamodb.Table(auto_table_name)
        scan = table.scan()
        items = scan.get("Items", [])
        assert len(items) == 2
        names = sorted(i["name"] for i in items)
        assert names == ["Alice", "Bob"]

        # Verify key schema
        table.load()
        assert len(table.key_schema) == 1
        assert table.key_schema[0]["AttributeName"] == "id"
        assert table.key_schema[0]["KeyType"] == "HASH"
    finally:
        try:
            dynamodb.Table(auto_table_name).delete()
        except Exception:
            pass


def test_create_table_composite_key(spark):
    """create_table with hash_key + range_key should create a composite key table."""
    import boto3

    auto_table_name = f"{TABLE_NAME}_autocreate_comp"
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)

    # Ensure table doesn't exist
    try:
        t = dynamodb.Table(auto_table_name)
        t.delete()
        t.wait_until_not_exists()
    except Exception:
        pass

    try:
        data = [("u1", "2025-01", "event_a"), ("u1", "2025-02", "event_b")]
        df = spark.createDataFrame(data, ["pk", "sk", "payload"])

        opts = _spark_options(auto_table_name)
        writer = df.write.format("dynamodb").mode("append")
        for k, v in opts.items():
            writer = writer.option(k, v)
        writer.option("create_table", "true") \
              .option("hash_key", "pk") \
              .option("range_key", "sk") \
              .save()

        time.sleep(2)

        table = dynamodb.Table(auto_table_name)
        scan = table.scan()
        assert len(scan.get("Items", [])) == 2

        # Verify composite key schema
        table.load()
        assert len(table.key_schema) == 2
        key_map = {k["KeyType"]: k["AttributeName"] for k in table.key_schema}
        assert key_map["HASH"] == "pk"
        assert key_map["RANGE"] == "sk"
    finally:
        try:
            dynamodb.Table(auto_table_name).delete()
        except Exception:
            pass


def test_create_table_skips_if_exists(spark, aws_table):
    """create_table should be a no-op when the table already exists."""
    _clear_table(aws_table)

    data = [("skip-001", "Alice", 30, 100)]
    df = spark.createDataFrame(data, ["id", "name", "age", "score"])

    opts = _spark_options(TABLE_NAME)
    writer = df.write.format("dynamodb").mode("append")
    for k, v in opts.items():
        writer = writer.option(k, v)
    writer.option("create_table", "true") \
          .option("hash_key", "id") \
          .save()

    time.sleep(2)

    resp = aws_table.get_item(Key={"id": "skip-001"})
    assert resp["Item"]["name"] == "Alice"


def test_write_without_create_table_fails_on_missing_table(spark):
    """Writing to a non-existent table without create_table should fail."""
    nonexistent = f"{TABLE_NAME}_does_not_exist"

    data = [("x-001", "Alice")]
    df = spark.createDataFrame(data, ["id", "name"])

    opts = _spark_options(nonexistent)
    writer = df.write.format("dynamodb").mode("append")
    for k, v in opts.items():
        writer = writer.option(k, v)

    with pytest.raises(Exception):
        writer.save()


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


def test_stream_write_with_delete_flag(spark, aws_table):
    """Stream write with delete_flag_column should delete matching items."""
    import pyspark.sql.functions as F

    _clear_table(aws_table)

    # Pre-insert items that the stream will delete
    aws_table.put_item(Item={"id": "sdel-0", "name": "ToDelete0"})
    aws_table.put_item(Item={"id": "sdel-1", "name": "ToDelete1"})
    aws_table.put_item(Item={"id": "sdel-2", "name": "ToDelete2"})
    time.sleep(1)

    # Rate source produces (timestamp, value). We derive an id that matches a
    # pre-inserted item and set the delete flag to "yes" for every row.
    stream_df = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", 5)
        .option("numPartitions", 1)
        .load()
        .withColumn("id", F.concat(F.lit("sdel-"), (F.col("value") % 3).cast("string")))
        .withColumn("name", F.lit("streamed"))
        .withColumn("del_flag", F.lit("yes"))
        .select("id", "name", "del_flag")
    )

    opts = _spark_options(TABLE_NAME)
    query = stream_df.writeStream.format("dynamodb").outputMode("append")
    for k, v in opts.items():
        query = query.option(k, v)
    query = (
        query
        .option("delete_flag_column", "del_flag")
        .option("delete_flag_value", "yes")
        .option("checkpointLocation", f"/tmp/spark_ddb_del_test_{uuid.uuid4().hex[:8]}")
    )
    running = query.start()

    try:
        time.sleep(10)
    finally:
        running.stop()

    # All three pre-inserted items should have been deleted
    for i in range(3):
        resp = aws_table.get_item(Key={"id": f"sdel-{i}"})
        assert "Item" not in resp, f"Expected sdel-{i} to be deleted"
