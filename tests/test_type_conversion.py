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


# --- EAV mixed-type value column matrix ---------------------------------
# An entity-attribute-value table stores heterogeneous values in one column.
# These lock the per-type write conversions so the pattern is safe.


def test_convert_for_dynamodb_int_passthrough():
    """int stays int (DynamoDB N)."""
    from dynamodb_data_source.type_conversion import convert_for_dynamodb

    result = convert_for_dynamodb(42)
    assert result == 42
    assert isinstance(result, int)


def test_convert_for_dynamodb_bool_stays_bool():
    """bool must stay bool (DynamoDB BOOL), not become a number.

    bool is a subclass of int but not float, so it must not be coerced.
    """
    from dynamodb_data_source.type_conversion import convert_for_dynamodb

    assert convert_for_dynamodb(True) is True
    assert convert_for_dynamodb(False) is False


def test_convert_for_dynamodb_decimal_passthrough():
    """An existing Decimal passes through unchanged."""
    from dynamodb_data_source.type_conversion import convert_for_dynamodb

    value = Decimal("3.14")
    result = convert_for_dynamodb(value)
    assert result == value
    assert isinstance(result, Decimal)


def test_convert_for_dynamodb_string_passthrough():
    """str passes through (DynamoDB S)."""
    from dynamodb_data_source.type_conversion import convert_for_dynamodb

    assert convert_for_dynamodb("hello") == "hello"


def test_convert_for_dynamodb_list_mixed_types():
    """A list of mixed types converts each element."""
    from dynamodb_data_source.type_conversion import convert_for_dynamodb

    result = convert_for_dynamodb([1, 2.5, "three", True, None])
    assert result[0] == 1 and isinstance(result[0], int)
    assert isinstance(result[1], Decimal)
    assert result[2] == "three"
    assert result[3] is True
    assert result[4] is None


def test_convert_for_dynamodb_eav_value_column():
    """The same 'value' key can hold a different type per row."""
    from dynamodb_data_source.type_conversion import convert_for_dynamodb

    rows = [
        {"persona_id": "p1", "attribute_name": "age", "value": 30},
        {"persona_id": "p1", "attribute_name": "score", "value": 9.5},
        {"persona_id": "p1", "attribute_name": "name", "value": "Alice"},
        {"persona_id": "p1", "attribute_name": "active", "value": True},
    ]
    converted = [convert_for_dynamodb(r) for r in rows]

    assert converted[0]["value"] == 30 and isinstance(converted[0]["value"], int)
    assert isinstance(converted[1]["value"], Decimal)
    assert converted[2]["value"] == "Alice"
    assert converted[3]["value"] is True
