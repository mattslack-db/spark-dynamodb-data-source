"""Tests for DynamoDB schema derivation."""

from decimal import Decimal
from pyspark.sql.types import (
    StructType, StringType, LongType, DoubleType,
    BooleanType, BinaryType, ArrayType
)


def test_infer_spark_type_string():
    """Test string value infers StringType."""
    from dynamodb_data_source.schema import infer_spark_type

    assert infer_spark_type("hello") == StringType()


def test_infer_spark_type_int():
    """Test int value infers LongType."""
    from dynamodb_data_source.schema import infer_spark_type

    assert infer_spark_type(42) == LongType()


def test_infer_spark_type_decimal_whole():
    """Test whole Decimal infers LongType."""
    from dynamodb_data_source.schema import infer_spark_type

    assert infer_spark_type(Decimal("42")) == LongType()


def test_infer_spark_type_decimal_fractional():
    """Test fractional Decimal infers DoubleType."""
    from dynamodb_data_source.schema import infer_spark_type

    assert infer_spark_type(Decimal("3.14")) == DoubleType()


def test_infer_spark_type_bool():
    """Test bool infers BooleanType."""
    from dynamodb_data_source.schema import infer_spark_type

    assert infer_spark_type(True) == BooleanType()


def test_infer_spark_type_bytes():
    """Test bytes infers BinaryType."""
    from dynamodb_data_source.schema import infer_spark_type

    assert infer_spark_type(b"data") == BinaryType()


def test_infer_spark_type_list():
    """Test list infers ArrayType."""
    from dynamodb_data_source.schema import infer_spark_type

    result = infer_spark_type(["a", "b"])
    assert isinstance(result, ArrayType)
    assert result.elementType == StringType()


def test_infer_spark_type_none():
    """Test None falls back to StringType."""
    from dynamodb_data_source.schema import infer_spark_type

    assert infer_spark_type(None) == StringType()


def test_derive_schema_from_items():
    """Test deriving schema from DynamoDB items."""
    from dynamodb_data_source.schema import derive_schema_from_items

    items = [
        {"id": "abc-123", "name": "Alice", "age": Decimal("30")},
        {"id": "abc-456", "name": "Bob", "age": Decimal("25")},
    ]

    schema = derive_schema_from_items(items)

    assert isinstance(schema, StructType)
    assert len(schema.fields) == 3

    # Fields sorted alphabetically
    assert schema.fields[0].name == "age"
    assert schema.fields[0].dataType == LongType()
    assert schema.fields[1].name == "id"
    assert schema.fields[1].dataType == StringType()
    assert schema.fields[2].name == "name"
    assert schema.fields[2].dataType == StringType()


def test_derive_schema_from_items_empty():
    """Test deriving schema from empty items list."""
    from dynamodb_data_source.schema import derive_schema_from_items

    schema = derive_schema_from_items([])
    assert schema == StructType([])


def test_derive_schema_handles_type_conflict():
    """Test schema derivation falls back to StringType on type conflict."""
    from dynamodb_data_source.schema import derive_schema_from_items

    items = [
        {"id": "abc-123", "value": Decimal("42")},      # LongType
        {"id": "abc-456", "value": "not a number"},      # StringType
    ]

    schema = derive_schema_from_items(items)

    # "value" should fall back to StringType due to conflict
    value_field = [f for f in schema.fields if f.name == "value"][0]
    assert value_field.dataType == StringType()
