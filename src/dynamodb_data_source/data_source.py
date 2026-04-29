"""DynamoDB Data Source implementation."""

from pyspark.sql.datasource import DataSource

from .reader import DynamoDbBatchReader
from .writer import DynamoDbBatchWriter, DynamoDbStreamWriter


class DynamoDbDataSource(DataSource):
    """PySpark Data Source for AWS DynamoDB."""

    @classmethod
    def name(cls):
        """Return the data source format name."""
        return "dynamodb"

    def __init__(self, options):
        """Initialize data source with options."""
        self.options = options
        self._stream_writer = None

    def schema(self):
        """
        Return the schema of the data source.

        Connects to DynamoDB on the driver to derive schema from table items.
        This runs only once on the driver, never in forked worker processes.
        """
        from .schema import derive_schema_from_items

        import boto3
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BinaryType

        table_name = self.options["table_name"]
        aws_region = self.options["aws_region"]
        aws_access_key_id = self.options.get("aws_access_key_id")
        aws_secret_access_key = self.options.get("aws_secret_access_key")
        aws_session_token = self.options.get("aws_session_token")
        credential_name = self.options.get("credential_name")

        session_kwargs = {"region_name": aws_region}
        if credential_name:
            from .credentials import _driver_dbutils
            try:
                session_kwargs["botocore_session"] = (
                    _driver_dbutils().credentials.getServiceCredentialsProvider(credential_name)
                )
            except Exception as e:
                raise RuntimeError(
                    "Cannot derive schema from DynamoDB on the driver when "
                    "`credential_name` is set: the driver-side `dbutils` is not "
                    "reachable from the Spark Python data source callback "
                    "process. Pass an explicit schema via `.schema(...)` on the "
                    "reader to avoid driver-side schema inference."
                ) from e
        else:
            if aws_access_key_id:
                session_kwargs["aws_access_key_id"] = aws_access_key_id
            if aws_secret_access_key:
                session_kwargs["aws_secret_access_key"] = aws_secret_access_key
            if aws_session_token:
                session_kwargs["aws_session_token"] = aws_session_token

        session = boto3.Session(**session_kwargs)

        resource_kwargs = {}
        if self.options.get("endpoint_url"):
            resource_kwargs["endpoint_url"] = self.options["endpoint_url"]

        dynamodb = session.resource("dynamodb", **resource_kwargs)
        table = dynamodb.Table(table_name)

        # Sample items to derive schema
        response = table.scan(Limit=100)
        items = response.get("Items", [])

        if items:
            return derive_schema_from_items(items)

        # Empty table - derive schema from key schema and attribute definitions
        table.load()
        attr_type_map = {"S": StringType(), "N": DoubleType(), "B": BinaryType()}
        fields = []
        for attr_def in table.attribute_definitions:
            spark_type = attr_type_map.get(attr_def["AttributeType"], StringType())
            fields.append(StructField(attr_def["AttributeName"], spark_type, nullable=True))
        return StructType(sorted(fields, key=lambda f: f.name))

    def reader(self, schema):
        """Return a batch reader instance."""
        return DynamoDbBatchReader(self.options, schema)

    def writer(self, schema, overwrite):
        """Return a batch writer instance."""
        writer = DynamoDbBatchWriter(self.options, schema)
        writer.initialize()
        return writer

    def streamWriter(self, schema, overwrite):
        """Return a streaming writer instance, creating and initializing on first call."""
        if self._stream_writer is None:
            print("Initializing streaming writer")
            self._stream_writer = DynamoDbStreamWriter(self.options, schema)
            self._stream_writer.initialize()
        return self._stream_writer
