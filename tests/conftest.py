import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from unittest.mock import MagicMock


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("dynamodb-tests") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def basic_options():
    """Basic connection options for testing."""
    return {
        "table_name": "test_table",
        "aws_region": "us-east-1",
        "endpoint_url": "http://localhost:8000",
    }


@pytest.fixture
def sample_schema():
    """Sample Spark schema for testing."""
    return StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("score", LongType(), True)
    ])


@pytest.fixture
def mock_dynamodb_table():
    """Mock DynamoDB table with key schema."""
    table = MagicMock()
    table.table_name = "test_table"
    table.key_schema = [
        {"AttributeName": "id", "KeyType": "HASH"},
    ]
    table.attribute_definitions = [
        {"AttributeName": "id", "AttributeType": "S"},
    ]
    table.item_count = 3
    table.load = MagicMock()

    # Mock scan for schema derivation (returns sample items)
    table.scan.return_value = {
        "Items": [
            {"id": "abc-123", "name": "Alice", "age": 30, "score": 100},
            {"id": "abc-456", "name": "Bob", "age": 25, "score": 200},
        ]
    }

    return table
