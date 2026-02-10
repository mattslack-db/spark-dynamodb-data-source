"""Type conversion utilities for DynamoDB data types."""

from decimal import Decimal


def convert_dynamodb_value(value):
    """
    Convert a DynamoDB value to a Spark-compatible Python type.

    DynamoDB (via boto3 resource) returns:
        - Numbers as Decimal
        - Sets as set
        - Binary as boto3.dynamodb.types.Binary or bytes

    Args:
        value: Value from a DynamoDB item

    Returns:
        Converted value suitable for Spark
    """
    if value is None:
        return None

    # Convert Decimal to int or float
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)

    # Convert sets to lists (Spark has no set type)
    if isinstance(value, set):
        return [convert_dynamodb_value(v) for v in value]

    # Convert nested dicts
    if isinstance(value, dict):
        return {k: convert_dynamodb_value(v) for k, v in value.items()}

    # Convert nested lists
    if isinstance(value, list):
        return [convert_dynamodb_value(v) for v in value]

    # Pass through str, bool, int, float, bytes
    return value


def convert_for_dynamodb(value):
    """
    Convert a Spark/Python value for writing to DynamoDB.

    DynamoDB requires Decimal instead of float, and doesn't accept
    empty strings for key attributes.

    Args:
        value: Value from a Spark Row

    Returns:
        Converted value suitable for DynamoDB put_item
    """
    if value is None:
        return None

    if isinstance(value, float):
        return Decimal(str(value))

    if isinstance(value, dict):
        return {k: convert_for_dynamodb(v) for k, v in value.items()}

    if isinstance(value, list):
        return [convert_for_dynamodb(v) for v in value]

    return value
