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

        session_kwargs = {"region_name": aws_region}
        if self.options.get("aws_access_key_id"):
            session_kwargs["aws_access_key_id"] = self.options["aws_access_key_id"]
        if self.options.get("aws_secret_access_key"):
            session_kwargs["aws_secret_access_key"] = self.options["aws_secret_access_key"]
        if self.options.get("aws_session_token"):
            session_kwargs["aws_session_token"] = self.options["aws_session_token"]

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
        return DynamoDbBatchWriter(self.options, schema)

    def streamWriter(self, schema, overwrite):
        """Return a streaming writer instance."""
        return DynamoDbStreamWriter(self.options, schema)
