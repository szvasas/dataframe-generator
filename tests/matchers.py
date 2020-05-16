from dataframe_generator.main import StructField, StructType


def assert_struct_field_equals(expected: StructField, actual: StructField):
    assert expected.name == actual.name
    assert expected.data_type == actual.data_type
    assert expected.nullable == actual.nullable


def assert_struct_type_equals(expected: StructType, actual: StructType):
    assert expected.name == actual.name
    for i in range(len(expected.fields)):
        assert_struct_field_equals(expected.fields[i], actual.fields[i])
