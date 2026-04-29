"""DynamoDB reader implementations using boto3."""

from pyspark.sql.datasource import DataSourceReader

from .credentials import _driver_dbutils
from .partitioning import SegmentPartition
from .type_conversion import convert_dynamodb_value


class DynamoDbReader:
    """Base reader class for DynamoDB data sources.

    IMPORTANT: The reader __init__ must NOT connect to DynamoDB (no boto3 calls).
    PySpark re-instantiates the reader in a forked Python worker process for
    partitions() and read(). Making boto3/SSL connections in __init__ causes the
    forked child process to crash due to non-fork-safe SSL state.

    Schema derivation (which needs a DynamoDB connection) is handled by
    DynamoDbDataSource.schema() on the driver before the reader is created.
    """

    def __init__(self, options, schema):
        """
        Initialize reader with pre-resolved schema.

        Args:
            options: Configuration options dict
            schema: Spark StructType schema (already resolved by DataSource.schema())
        """
        self.options = options

        # Validate required options
        self._validate_options()

        # Extract connection options (stored for use in read())
        self.table_name = options["table_name"]
        self.aws_region = options["aws_region"]
        self.aws_access_key_id = options.get("aws_access_key_id")
        self.aws_secret_access_key = options.get("aws_secret_access_key")
        self.aws_session_token = options.get("aws_session_token")
        self.endpoint_url = options.get("endpoint_url")
        self.credential_name = options.get("credential_name")

        # Read options
        self.total_segments = int(options.get("total_segments", 1))
        self.consistent_read = options.get("consistent_read", "false").lower() == "true"

        # Schema is always provided (resolved by DataSource.schema() or user)
        self.schema = schema
        self.columns = [field.name for field in schema.fields] if schema else []

    def _validate_options(self):
        """Validate required options are present."""
        required = ["table_name", "aws_region"]
        missing = [opt for opt in required if opt not in self.options]

        if missing:
            raise ValueError(f"Missing required options: {', '.join(missing)}")

    def _get_botocore_session(self):
        """Return an auto-refreshing botocore Session for the UC service credential.

        Picks the right API based on where this code is running:
        - Executor (inside a TaskContext / Python UDF): use
          `databricks.service_credentials.getServiceCredentialsProvider`.
        - Driver (notebook / Spark application): use
          `dbutils.credentials.getServiceCredentialsProvider`.

        Returns None when no credential_name was provided.
        """
        if not self.credential_name:
            return None

        from pyspark import TaskContext

        if TaskContext.get() is not None:
            from databricks.service_credentials import getServiceCredentialsProvider
            return getServiceCredentialsProvider(self.credential_name)

        return _driver_dbutils().credentials.getServiceCredentialsProvider(self.credential_name)

    def _get_resource(self):
        """Create boto3 DynamoDB resource."""
        import boto3

        session_kwargs = {"region_name": self.aws_region}

        botocore_session = self._get_botocore_session()
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

    def partitions(self):
        """
        Return list of partitions for parallel reading.

        Uses DynamoDB parallel scan with Segment/TotalSegments.

        Returns:
            List of SegmentPartition objects
        """
        return [SegmentPartition(i, self.total_segments) for i in range(self.total_segments)]

    def read(self, partition):
        """
        Read data from a DynamoDB table segment using Scan.

        Args:
            partition: SegmentPartition to read

        Yields:
            Tuples representing rows in schema column order
        """
        dynamodb = self._get_resource()
        table = dynamodb.Table(self.table_name)

        scan_kwargs = {
            "ConsistentRead": self.consistent_read,
        }

        # Only use parallel scan params if total_segments > 1
        if partition.total_segments > 1:
            scan_kwargs["Segment"] = partition.segment
            scan_kwargs["TotalSegments"] = partition.total_segments

        # Column projection with ExpressionAttributeNames for reserved keywords
        if self.columns:
            expr_attr_names = {}
            projection_parts = []
            for col in self.columns:
                alias = f"#{col}"
                expr_attr_names[alias] = col
                projection_parts.append(alias)
            scan_kwargs["ProjectionExpression"] = ", ".join(projection_parts)
            scan_kwargs["ExpressionAttributeNames"] = expr_attr_names

        # Paginate through all results
        while True:
            response = table.scan(**scan_kwargs)

            for item in response.get("Items", []):
                # Convert values and yield tuple in schema column order
                values = tuple(convert_dynamodb_value(item.get(col)) for col in self.columns)
                yield values

            # Check for pagination
            if "LastEvaluatedKey" not in response:
                break
            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]


class DynamoDbBatchReader(DynamoDbReader, DataSourceReader):
    """Batch reader for DynamoDB."""

    pass
