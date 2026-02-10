"""Tests for DynamoDB type conversion utilities."""

from decimal import Decimal


def test_convert_decimal_whole_to_int():
    """Test whole Decimal converts to int."""
    from dynamodb_data_source.type_conversion import convert_dynamodb_value

    result = convert_dynamodb_value(Decimal("42"))
    assert result == 42
    assert isinstance(result, int)


def test_convert_decimal_fractional_to_float():
    """Test fractional Decimal converts to float."""
    from dynamodb_data_source.type_conversion import convert_dynamodb_value

    result = convert_dynamodb_value(Decimal("3.14"))
    assert abs(result - 3.14) < 0.001
    assert isinstance(result, float)


def test_convert_set_to_list():
    """Test set converts to list."""
    from dynamodb_data_source.type_conversion import convert_dynamodb_value

    result = convert_dynamodb_value({"a", "b", "c"})
    assert isinstance(result, list)
    assert set(result) == {"a", "b", "c"}


def test_convert_none():
    """Test None passes through."""
    from dynamodb_data_source.type_conversion import convert_dynamodb_value

    assert convert_dynamodb_value(None) is None


def test_convert_string_passthrough():
    """Test string passes through."""
    from dynamodb_data_source.type_conversion import convert_dynamodb_value

    assert convert_dynamodb_value("hello") == "hello"


def test_convert_bool_passthrough():
    """Test bool passes through."""
    from dynamodb_data_source.type_conversion import convert_dynamodb_value

    assert convert_dynamodb_value(True) is True


def test_convert_nested_dict():
    """Test nested dict with Decimal values."""
    from dynamodb_data_source.type_conversion import convert_dynamodb_value

    result = convert_dynamodb_value({"count": Decimal("10"), "name": "test"})
    assert result == {"count": 10, "name": "test"}
    assert isinstance(result["count"], int)


def test_convert_nested_list():
    """Test nested list with Decimal values."""
    from dynamodb_data_source.type_conversion import convert_dynamodb_value

    result = convert_dynamodb_value([Decimal("1"), Decimal("2.5")])
    assert result == [1, 2.5]
    assert isinstance(result[0], int)
    assert isinstance(result[1], float)


def test_convert_for_dynamodb_float():
    """Test float to Decimal conversion for DynamoDB writes."""
    from dynamodb_data_source.type_conversion import convert_for_dynamodb

    result = convert_for_dynamodb(3.14)
    assert isinstance(result, Decimal)
    assert float(result) == 3.14


def test_convert_for_dynamodb_none():
    """Test None passes through for DynamoDB writes."""
    from dynamodb_data_source.type_conversion import convert_for_dynamodb

    assert convert_for_dynamodb(None) is None


def test_convert_for_dynamodb_nested():
    """Test nested dict conversion for DynamoDB writes."""
    from dynamodb_data_source.type_conversion import convert_for_dynamodb

    result = convert_for_dynamodb({"price": 19.99, "name": "item"})
    assert isinstance(result["price"], Decimal)
    assert result["name"] == "item"
