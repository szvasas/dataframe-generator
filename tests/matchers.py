import json
from typing import Dict

from dataframe_generator.data_type import DataType, DecimalType
from dataframe_generator.struct_field import StructField
from dataframe_generator.struct_type import StructType


def assert_struct_field_equals(expected: StructField, actual: StructField):
    assert expected.name == actual.name
    assert_data_type(expected.data_type, actual.data_type)
    assert expected.nullable == actual.nullable


def assert_data_type(expected: DataType, actual: DataType):
    expected_type = type(expected)
    if expected_type is not DecimalType:
        assert isinstance(expected, type(actual))
    else:
        assert expected.scale == actual.scale
        assert expected.precision == actual.precision


def assert_struct_type_equals(expected: StructType, actual: StructType):
    assert expected.name == actual.name
    for i in range(len(expected.fields)):
        assert_struct_field_equals(expected.fields[i], actual.fields[i])


def assert_dict_equals(expected: Dict, actual: Dict):
    expected_string = json.dumps(expected)
    actual_string = json.dumps(actual)
    assert expected_string == actual_string
