# Python Data Source for AWS DynamoDB

Python Data Source for Apache Spark enabling batch and streaming reads/writes to AWS DynamoDB.

## Features

- **Batch Writes**: Write DataFrames to DynamoDB tables with batch_writer
- **Streaming Writes**: Write streaming DataFrames with micro-batch processing
- **Batch Reads**: Parallel scan with segment-based partitioning
- **Schema Derivation**: Auto-infer schema from table items, or provide explicit schema for projection
- **Delete Flag Support**: Conditional row deletion during writes
- **Primary Key Validation**: Early validation ensures DataFrame contains all key columns
- **Float/Decimal Handling**: Automatic float-to-Decimal conversion for writes
- **Unity Catalog Service Credentials**: Authenticate via Databricks service credentials

## Installation

```bash
poetry install
```

## Quick Start

### Batch Write

```python
from pyspark.sql import SparkSession
from dynamodb_data_source import DynamoDbDataSource

spark = SparkSession.builder.appName("dynamodb").getOrCreate()
spark.dataSource.register(DynamoDbDataSource)

df = spark.createDataFrame([
    ("id-001", "Alice", 30),
    ("id-002", "Bob", 25)
], ["id", "name", "age"])

df.write.format("dynamodb") \
    .mode("append") \
    .option("table_name", "users") \
    .option("aws_region", "us-east-1") \
    .save()
```

### Streaming Write

```python
df.writeStream.format("dynamodb") \
    .option("table_name", "users") \
    .option("aws_region", "us-east-1") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()
```

### Write with Delete Flag

```python
df.write.format("dynamodb") \
    .mode("append") \
    .option("table_name", "users") \
    .option("aws_region", "us-east-1") \
    .option("delete_flag_column", "is_deleted") \
    .option("delete_flag_value", "true") \
    .save()
```

### Batch Read

```python
from pyspark.sql import SparkSession
from dynamodb_data_source import DynamoDbDataSource

spark = SparkSession.builder.appName("dynamodb").getOrCreate()
spark.dataSource.register(DynamoDbDataSource)

# Read entire table
df = spark.read.format("dynamodb") \
    .option("table_name", "users") \
    .option("aws_region", "us-east-1") \
    .load()

df.show()
```

### Read with Parallel Scan

```python
# Use 4 parallel scan segments for faster reads
df = spark.read.format("dynamodb") \
    .option("table_name", "users") \
    .option("aws_region", "us-east-1") \
    .option("total_segments", "4") \
    .load()
```

### Read with Schema (Column Projection)

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("age", IntegerType())
])

df = spark.read.format("dynamodb") \
    .schema(schema) \
    .option("table_name", "users") \
    .option("aws_region", "us-east-1") \
    .load()
```

### Using Unity Catalog Service Credentials (Databricks)

On Databricks, you can authenticate using a Unity Catalog service credential instead of explicit AWS keys:

```python
from databricks.sdk.runtime import dbutils

uc_service_credential_name = "<your-service-credential-name>"
provider = dbutils.credentials.getServiceCredentialsProvider(uc_service_credential_name)
credentials = provider.get_credentials().get_frozen_credentials()

uc_options = {
    "table_name": "<your-table-name>",
    "aws_region": "us-east-1",
    "aws_access_key_id": credentials.access_key,
    "aws_secret_access_key": credentials.secret_key,
    "aws_session_token": credentials.token,
    "credential_name": uc_service_credential_name,
}

# Write
df.write.format("dynamodb").mode("append").options(**uc_options).save()

# Read
df = spark.read.format("dynamodb").options(**uc_options).load()
```

#### Compute requirements and the explicit-schema caveat

Validated end-to-end on classic DBR **17.3 LTS** (Spark 4.0) and serverless environment **v3** (`client.3.6`). Service credentials require DBR 16.2+ on classic, or serverless env v3+. On serverless, the wheel must be installed notebook-scoped (`%pip install` or the Environment side panel) — compute-scoped libraries are not supported there.

**Pass an explicit `.schema(...)` whenever you use `credential_name`** — on both classic and serverless. The Python Data Source's `schema()` callback runs in a separate Python child process from the notebook, where `dbutils` is unreachable and `databricks.service_credentials.getServiceCredentialsProvider()` rejects the call (it requires a UDF context). Without an explicit schema you will get a `RuntimeError` telling you to add one. With one, schema inference is skipped and only the executor `read()`/`write()` callbacks run, which use the executor-side API correctly.

```python
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("age", LongType()),
])

df = (spark.read.format("dynamodb")
      .schema(schema)
      .options(**uc_options)
      .load())
```

The write path needs no change — `df.write.format("dynamodb").options(**uc_options).save()` works as-is on both compute types, and credentials auto-refresh via the botocore `RefreshableCredentials` returned by the executor-side API.

## Configuration Options

### Connection Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `table_name` | Yes | - | DynamoDB table name |
| `aws_region` | Yes | - | AWS region (e.g., us-east-1) |
| `aws_access_key_id` | No | - | AWS access key ID (uses default credentials if not set) |
| `aws_secret_access_key` | No | - | AWS secret access key |
| `aws_session_token` | No | - | AWS session token for temporary credentials |
| `endpoint_url` | No | - | Custom endpoint URL (e.g., http://localhost:8000 for DynamoDB Local) |
| `credential_name` | No | - | Databricks Unity Catalog service credential name for AWS authentication |

### Write Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `delete_flag_column` | No | - | Column indicating deletion (must be used with delete_flag_value) |
| `delete_flag_value` | No | - | Value triggering deletion (must be used with delete_flag_column) |
| `create_table` | No | `false` | Create the DynamoDB table if it doesn't exist |
| `hash_key` | No* | - | Hash key column name (required when `create_table` is `true`) |
| `range_key` | No | - | Range key column name |
| `billing_mode` | No | `PAY_PER_REQUEST` | Billing mode for table creation (`PAY_PER_REQUEST` or `PROVISIONED`) |

### Read Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `total_segments` | No | 1 | Number of parallel scan segments |
| `consistent_read` | No | false | Use strongly consistent reads |

## Development

### Setup

```bash
poetry install
```

### Run Tests

```bash
# Unit tests only
poetry run pytest -v -m "not integration"

# Integration tests (requires DynamoDB Local)
cd tests && docker-compose up -d
poetry run pytest -v -m integration
cd tests && docker-compose down
```

### Code Quality

```bash
poetry run ruff check src/
poetry run ruff format src/
poetry run mypy src/
```

## License

MIT
