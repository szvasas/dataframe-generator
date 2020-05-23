from dataframe_generator.data_type import StringType
from dataframe_generator.generator import generate_values
from dataframe_generator.struct_field import StructField
from dataframe_generator.struct_type import StructType
from tests.matchers import assert_dict_equals


def test_preset_values():
    expected = {
        'field_names': ['name'],
        'values': [
            ['preset_name_1'],
            ['preset_name_2'],
            ['preset_name_1']
        ]
    }

    struct_type = StructType('test_type', [StructField('name', StringType(), False)])
    actual = generate_values(3, struct_type, {"name": ["preset_name_1", "preset_name_2"]})

    assert_dict_equals(expected, actual)
