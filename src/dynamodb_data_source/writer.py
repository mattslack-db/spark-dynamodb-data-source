"""DynamoDB writer implementations using boto3."""

from pyspark.sql.datasource import DataSourceWriter, DataSourceStreamWriter

from .credentials import get_botocore_session
from .rate_limiter import TokenBucketRateLimiter
from .type_conversion import convert_for_dynamodb


class DynamoDbWriter:
    """Base writer class with shared write logic for DynamoDB."""

    def __init__(self, options, schema):
        """Initialize writer and validate configuration."""
        self.options = options
        self.schema = schema

        # Validate required options
        self._validate_options()

        # Extract connection options
        self.table_name = options["table_name"]
        self.aws_region = options["aws_region"]
        self.aws_access_key_id = options.get("aws_access_key_id")
        self.aws_secret_access_key = options.get("aws_secret_access_key")
        self.aws_session_token = options.get("aws_session_token")
        self.endpoint_url = options.get("endpoint_url")
        self.credential_name = options.get("credential_name")

        # Write options
        self.delete_flag_column = options.get("delete_flag_column")
        self.delete_flag_value = options.get("delete_flag_value")
        self.create_table = options.get("create_table", "false").lower() == "true"
        self.hash_key_name = options.get("hash_key")
        self.range_key_name = options.get("range_key")
        self.billing_mode = options.get("billing_mode", "PAY_PER_REQUEST")

        # Optional per-partition write throughput limit (items/sec). Spark runs
        # write() independently per partition, so the effective global rate is
        # roughly max_writes_per_second * numPartitions.
        raw_rate_limit = options.get("max_writes_per_second")
        if raw_rate_limit is None:
            self.max_writes_per_second = None
        else:
            try:
                self.max_writes_per_second = float(raw_rate_limit)
            except (TypeError, ValueError):
                raise ValueError(
                    f"max_writes_per_second must be a number, got {raw_rate_limit!r}"
                )
            if self.max_writes_per_second <= 0:
                raise ValueError(
                    f"max_writes_per_second must be positive, got {self.max_writes_per_second}"
                )

        # Validate delete flag options
        if bool(self.delete_flag_column) != bool(self.delete_flag_value):
            raise ValueError(
                "Both delete_flag_column and delete_flag_value must be specified together, or neither"
            )

        # Validate create_table options
        if self.create_table and not self.hash_key_name:
            raise ValueError("hash_key option is required when create_table is true")

    def initialize(self):
        """Driver-side init: create the table and load metadata when we can
        reach AWS from the driver.

        When `credential_name` is set the data source callback runs in a
        Python child process spawned by Spark, where notebook `dbutils` is not
        available, so we defer AWS calls to the first `write()` invocation
        on an executor (where the executor-side service credentials API is
        usable).
        """
        if self.credential_name:
            self._initialized = False
            return
        if self.create_table:
            self._create_table_if_not_exists()
        self._load_table_metadata()
        self._initialized = True

    def _ensure_initialized(self):
        """Lazy executor-side init for the `credential_name` path."""
        if getattr(self, "_initialized", False):
            return
        if self.create_table:
            self._create_table_if_not_exists()
        self._load_table_metadata()
        self._initialized = True

    def _validate_options(self):
        """Validate required options are present."""
        required = ["table_name", "aws_region"]
        missing = [opt for opt in required if opt not in self.options]

        if missing:
            raise ValueError(f"Missing required options: {', '.join(missing)}")

    def _get_resource(self):
        """Create boto3 DynamoDB resource."""
        import boto3

        session_kwargs = {"region_name": self.aws_region}

        botocore_session = get_botocore_session(self.credential_name)
        if botocore_session is not None:
            session_kwargs["botocore_session"] = botocore_session
        else:
            if self.aws_access_key_id:
                session_kwargs["aws_access_key_id"] = self.aws_access_key_id
            if self.aws_secret_access_key:
                session_kwargs["aws_secret_access_key"] = self.aws_secret_access_key
            if self.aws_session_token:
                session_kwargs["aws_session_token"] = self.aws_session_token

        session = boto3.Session(**session_kwargs)

        resource_kwargs = {}
        if self.endpoint_url:
            resource_kwargs["endpoint_url"] = self.endpoint_url

        return session.resource("dynamodb", **resource_kwargs)

    def _create_table_if_not_exists(self):
        """Create the DynamoDB table if it doesn't already exist."""
        import botocore.exceptions

        dynamodb = self._get_resource()

        try:
            table = dynamodb.Table(self.table_name)
            table.creation_date_time  # triggers DescribeTable; raises if not found
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] != "ResourceNotFoundException":
                raise

            # Build attribute type from Spark schema
            def get_attribute_type(spark_type):
                type_name = spark_type.typeName()
                if type_name in ("integer", "long", "float", "double", "decimal", "short", "byte"):
                    return "N"
                if type_name == "binary":
                    return "B"
                return "S"

            # Map schema columns by name for lookup
            schema_map = {field.name: field for field in self.schema.fields}

            attribute_definitions = []
            key_schema = []

            # Hash key
            if self.hash_key_name not in schema_map:
                raise ValueError(
                    f"hash_key '{self.hash_key_name}' not found in DataFrame schema"
                )
            attribute_definitions.append({
                "AttributeName": self.hash_key_name,
                "AttributeType": get_attribute_type(schema_map[self.hash_key_name].dataType),
            })
            key_schema.append({"AttributeName": self.hash_key_name, "KeyType": "HASH"})

            # Range key (optional)
            if self.range_key_name:
                if self.range_key_name not in schema_map:
                    raise ValueError(
                        f"range_key '{self.range_key_name}' not found in DataFrame schema"
                    )
                attribute_definitions.append({
                    "AttributeName": self.range_key_name,
                    "AttributeType": get_attribute_type(schema_map[self.range_key_name].dataType),
                })
                key_schema.append({"AttributeName": self.range_key_name, "KeyType": "RANGE"})

            table = dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attribute_definitions,
                BillingMode=self.billing_mode,
            )
            table.wait_until_exists()

    def _load_table_metadata(self):
        """Load key schema from DynamoDB and validate DataFrame schema."""
        dynamodb = self._get_resource()
        table = dynamodb.Table(self.table_name)
        table.load()

        # Extract key columns from key schema
        self.key_schema = table.key_schema  # [{"AttributeName": "id", "KeyType": "HASH"}, ...]
        self.hash_key = None
        self.range_key = None

        for key in self.key_schema:
            if key["KeyType"] == "HASH":
                self.hash_key = key["AttributeName"]
            elif key["KeyType"] == "RANGE":
                self.range_key = key["AttributeName"]

        # Validate DataFrame schema contains all key columns
        df_columns = set(field.name for field in self.schema.fields)
        key_columns = [k["AttributeName"] for k in self.key_schema]
        missing_keys = [k for k in key_columns if k not in df_columns]

        if missing_keys:
            raise ValueError(
                f"DataFrame schema missing key columns: {', '.join(missing_keys)}. "
                f"Required key columns: {', '.join(key_columns)}"
            )

        # Validate delete flag column exists if specified
        if self.delete_flag_column and self.delete_flag_column not in df_columns:
            raise ValueError(
                f"delete_flag_column '{self.delete_flag_column}' not found in DataFrame schema. "
                f"Available columns: {', '.join(sorted(df_columns))}"
            )

    def write(self, iterator):
        """
        Write data to DynamoDB using batch_writer.

        This runs on executors, so import boto3 here.
        """
        from pyspark.sql.datasource import WriterCommitMessage

        self._ensure_initialized()

        dynamodb = self._get_resource()
        table = dynamodb.Table(self.table_name)

        limiter = (
            TokenBucketRateLimiter(self.max_writes_per_second)
            if self.max_writes_per_second
            else None
        )

        row_count = 0

        # De-duplicate by the table's full primary key within each flushed
        # batch. A single BatchWriteItem call cannot reference the same key
        # twice ("Provided list of item keys contains duplicates"), which would
        # otherwise fail the whole batch when a partition or streaming
        # microbatch repeats a key (e.g. a CDC diff). boto3 keeps the last
        # operation per key per flush, matching DynamoDB's batch semantics.
        key_names = [k["AttributeName"] for k in self.key_schema]

        with table.batch_writer(overwrite_by_pkeys=key_names) as batch:
            for row in iterator:
                if limiter is not None:
                    limiter.acquire(1)

                row_dict = row.asDict(recursive=True)

                # Check if this is a delete
                is_delete = False
                if self.delete_flag_column:
                    flag_value = row_dict.get(self.delete_flag_column)
                    if str(flag_value).lower() == self.delete_flag_value.lower():
                        is_delete = True

                if is_delete:
                    # Build key for delete, converting values (e.g. float -> Decimal)
                    # so numeric keys work here just as they do on the PUT path.
                    key = {self.hash_key: convert_for_dynamodb(row_dict[self.hash_key])}
                    if self.range_key:
                        key[self.range_key] = convert_for_dynamodb(row_dict[self.range_key])

                    # Validate key values are not null
                    for k, v in key.items():
                        if v is None:
                            raise ValueError(f"Key column '{k}' cannot be null for DELETE")

                    batch.delete_item(Key=key)
                else:
                    # Remove delete flag column from item data
                    item = {k: v for k, v in row_dict.items() if k != self.delete_flag_column}

                    # Convert values for DynamoDB (e.g. float -> Decimal).
                    item = convert_for_dynamodb(item)

                    # Validate key columns are not null
                    for key_def in self.key_schema:
                        key_col = key_def["AttributeName"]
                        if item.get(key_col) is None:
                            raise ValueError(
                                f"Key column '{key_col}' cannot be null for INSERT (row {row_count})"
                            )

                    batch.put_item(Item=item)

                row_count += 1

        return WriterCommitMessage()


class DynamoDbBatchWriter(DynamoDbWriter, DataSourceWriter):
    """Batch writer for DynamoDB."""

    pass


class DynamoDbStreamWriter(DynamoDbWriter, DataSourceStreamWriter):
    """Streaming writer for DynamoDB."""

    def commit(self, messages, batch_id):
        """Handle successful batch completion."""
        pass

    def abort(self, messages, batch_id):
        """Handle failed batch."""
        pass
