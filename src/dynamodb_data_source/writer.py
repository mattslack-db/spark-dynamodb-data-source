"""DynamoDB writer implementations using boto3."""

from pyspark.sql.datasource import DataSourceWriter, DataSourceStreamWriter


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

        # Validate delete flag options
        if bool(self.delete_flag_column) != bool(self.delete_flag_value):
            raise ValueError(
                "Both delete_flag_column and delete_flag_value must be specified together, or neither"
            )

        # Validate create_table options
        if self.create_table and not self.hash_key_name:
            raise ValueError("hash_key option is required when create_table is true")

        # Resolve credentials, create table if needed, and load table metadata
        self._resolve_credentials()
        if self.create_table:
            self._create_table_if_not_exists()
        self._load_table_metadata()

    def _validate_options(self):
        """Validate required options are present."""
        required = ["table_name", "aws_region"]
        missing = [opt for opt in required if opt not in self.options]

        if missing:
            raise ValueError(f"Missing required options: {', '.join(missing)}")

    def _resolve_credentials(self):
        """Resolve AWS credentials from a Databricks Unity Catalog service credential.

        When credential_name is set, first tries databricks.service_credentials
        (available on newer Databricks runtimes). If that fails, falls back to
        the Databricks SDK generate-temporary-service-credential API.
        """
        if not self.credential_name:
            return

        try:
            import databricks.service_credentials

            provider = databricks.service_credentials.getServiceCredentialsProvider(self.credential_name)
            credentials = provider.get_credentials().get_frozen_credentials()

            self.aws_access_key_id = credentials.access_key
            self.aws_secret_access_key = credentials.secret_key
            self.aws_session_token = credentials.token
            print(f"AWS credentials refreshed using service credential '{self.credential_name}'")
        except Exception:
            print("Using AWS credentials as Lakeflow Connect service credentials are not available")

    def _get_resource(self):
        """Create boto3 DynamoDB resource."""
        import boto3

        session_kwargs = {"region_name": self.aws_region}

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

    def _convert_floats(self, obj):
        """Convert float values to Decimal for DynamoDB compatibility."""
        from decimal import Decimal

        if isinstance(obj, float):
            return Decimal(str(obj))
        if isinstance(obj, dict):
            return {k: self._convert_floats(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._convert_floats(item) for item in obj]
        return obj

    def write(self, iterator):
        """
        Write data to DynamoDB using batch_writer.

        This runs on executors, so import boto3 here.
        """
        from pyspark.sql.datasource import WriterCommitMessage

        dynamodb = self._get_resource()
        table = dynamodb.Table(self.table_name)

        row_count = 0

        with table.batch_writer() as batch:
            for row in iterator:
                row_dict = row.asDict(recursive=True)

                # Check if this is a delete
                is_delete = False
                if self.delete_flag_column:
                    flag_value = row_dict.get(self.delete_flag_column)
                    if str(flag_value).lower() == self.delete_flag_value.lower():
                        is_delete = True

                if is_delete:
                    # Build key for delete
                    key = {self.hash_key: row_dict[self.hash_key]}
                    if self.range_key:
                        key[self.range_key] = row_dict[self.range_key]

                    # Validate key values are not null
                    for k, v in key.items():
                        if v is None:
                            raise ValueError(f"Key column '{k}' cannot be null for DELETE")

                    batch.delete_item(Key=key)
                else:
                    # Remove delete flag column from item data
                    item = {k: v for k, v in row_dict.items() if k != self.delete_flag_column}

                    # Convert floats to Decimal (DynamoDB requires Decimal for numbers)
                    item = self._convert_floats(item)

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
