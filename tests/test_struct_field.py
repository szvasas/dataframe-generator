import pytest

from dataframe_generator.main import StructField
from tests.matchers import assert_struct_field_equals

test_data = [
    ("StructField('name12', LongType(), True),", StructField('name12', 'LongType()', True)),
    ("StructField('name12', LongType(), False),", StructField('name12', 'LongType()', False)),
    # ('StructField("name12", DecimalType(13, 2), False),', StructField('name12', 'DecimalType(13, 2)', False)),
    ('StructField(    "name12"    ,     StringType(),False    )', StructField('name12', 'StringType()', False)),
]


@pytest.mark.parametrize("raw_input, expected", test_data)
def test_parse(raw_input: str, expected: StructField):
    assert_struct_field_equals(expected, StructField.parse(raw_input))
