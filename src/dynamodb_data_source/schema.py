"""Schema derivation utilities for DynamoDB types."""

from decimal import Decimal

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, BooleanType, BinaryType, ArrayType, MapType
)


def infer_spark_type(value):
    """
    Infer Spark type from a Python value returned by DynamoDB.

    DynamoDB returns:
        str -> StringType
        Decimal -> LongType (whole numbers) or DoubleType (fractional)
        bool -> BooleanType
        bytes/bytearray -> BinaryType
        list -> ArrayType
        dict -> MapType
        set -> ArrayType
        None -> StringType (fallback)

    Args:
        value: A Python value from a DynamoDB item

    Returns:
        PySpark DataType
    """
    if value is None:
        return StringType()

    # Bool check must come before int (bool is subclass of int in Python)
    if isinstance(value, bool):
        return BooleanType()

    if isinstance(value, int):
        return LongType()

    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return LongType()
        return DoubleType()

    if isinstance(value, float):
        return DoubleType()

    if isinstance(value, str):
        return StringType()

    if isinstance(value, (bytes, bytearray)):
        return BinaryType()

    if isinstance(value, list):
        if value:
            return ArrayType(infer_spark_type(value[0]))
        return ArrayType(StringType())

    if isinstance(value, dict):
        return MapType(StringType(), StringType())

    if isinstance(value, set):
        if value:
            element = next(iter(value))
            return ArrayType(infer_spark_type(element))
        return ArrayType(StringType())

    # Default fallback
    return StringType()


def derive_schema_from_items(items):
    """
    Derive Spark schema from sample DynamoDB items.

    Scans all items to find every attribute and infer its type.
    If an attribute appears in multiple items with different types,
    falls back to StringType.

    Args:
        items: List of DynamoDB items (dicts)

    Returns:
        StructType representing the Spark schema
    """
    if not items:
        return StructType([])

    # Collect all attribute types
    attr_types = {}

    for item in items:
        for attr_name, attr_value in item.items():
            if attr_value is not None:
                inferred = infer_spark_type(attr_value)
                if attr_name in attr_types:
                    # If types conflict, fall back to StringType
                    if attr_types[attr_name] != inferred:
                        attr_types[attr_name] = StringType()
                else:
                    attr_types[attr_name] = inferred

    # Sort fields alphabetically for consistent ordering
    fields = []
    for name in sorted(attr_types.keys()):
        fields.append(StructField(name, attr_types[name], nullable=True))

    return StructType(fields)


def derive_schema_from_table(dynamodb_resource, table_name, endpoint_url=None):
    """
    Derive Spark schema from a DynamoDB table by sampling items.

    Scans a small sample of items to infer the full schema.
    Falls back to key-only schema if the table is empty.

    Args:
        dynamodb_resource: boto3 DynamoDB resource
        table_name: Name of the DynamoDB table
        endpoint_url: Optional endpoint URL (unused, kept for API compat)

    Returns:
        StructType representing the Spark schema
    """
    table = dynamodb_resource.Table(table_name)

    # Scan a sample of items to infer schema
    response = table.scan(Limit=100)
    items = response.get("Items", [])

    if items:
        return derive_schema_from_items(items)

    # Empty table - derive schema from key schema and attribute definitions
    table.load()
    attr_type_map = {
        "S": StringType(),
        "N": DoubleType(),
        "B": BinaryType(),
    }

    fields = []
    for attr_def in table.attribute_definitions:
        attr_name = attr_def["AttributeName"]
        attr_type = attr_def["AttributeType"]
        spark_type = attr_type_map.get(attr_type, StringType())
        fields.append(StructField(attr_name, spark_type, nullable=True))

    return StructType(sorted(fields, key=lambda f: f.name))
