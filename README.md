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

### Write Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `delete_flag_column` | No | - | Column indicating deletion (must be used with delete_flag_value) |
| `delete_flag_value` | No | - | Value triggering deletion (must be used with delete_flag_column) |

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

## Phase Status

### Phase 1: Write Operations ✅

- ✅ Batch writes with batch_writer
- ✅ Streaming writes
- ✅ AWS credential management
- ✅ Primary key validation from table metadata
- ✅ Float-to-Decimal conversion
- ✅ Delete flag support

### Phase 2: Batch Read Operations ✅

- ✅ Segment-based parallel scanning
- ✅ Schema derivation from sampled items
- ✅ Explicit schema support (column projection)
- ✅ Consistent read option
- ✅ Pagination handling

### Phase 3: Streaming Reads (Future)

- [ ] DynamoDB Streams integration
- [ ] Change event processing
- [ ] Offset management

## License

MIT
