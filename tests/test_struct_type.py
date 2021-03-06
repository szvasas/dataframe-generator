import pytest

from dataframe_generator.data_type import LongType, StringType, ByteType, IntegerType, DateType, TimestampType, \
    ShortType, DecimalType
from dataframe_generator.struct_field import StructField
from dataframe_generator.struct_type import StructType
from tests.matchers import assert_struct_type_equals

test_data = [
    ("""
        schemaname2 = StructType([
          StructField('name12', LongType(), True),
          StructField('name22', StringType(), True),
          StructField('name32', ByteType(), False),
          StructField('name42', IntegerType(), True),
          StructField('name52', DateType(), True),
          StructField('name62', TimestampType(), True),
          StructField('name72', ShortType(), False),
        ])""",
        StructType('schemaname2', [
          StructField('name12', LongType(), True),
          StructField('name22', StringType(), True),
          StructField('name32', ByteType(), False),
          StructField('name42', IntegerType(), True),
          StructField('name52', DateType(), True),
          StructField('name62', TimestampType(), True),
          StructField('name72', ShortType(), False),
        ])
     ),
    ("""
        my_cool_schema     =StructType([StructField('name12',LongType(),False),
          StructField('name22',StringType(), True),StructField('name32',ByteType(), False),
          StructField('name42',IntegerType(), True),         StructField('name52',DateType(), True),
          StructField("name62",TimestampType(),True),
          StructField('name72',ShortType(),False)
        ])""",
     StructType('my_cool_schema', [
         StructField('name12', LongType(), False),
         StructField('name22', StringType(), True),
         StructField('name32', ByteType(), False),
         StructField('name42', IntegerType(), True),
         StructField('name52', DateType(), True),
         StructField('name62', TimestampType(), True),
         StructField('name72', ShortType(), False),
     ])
     ),
]


@pytest.mark.parametrize("raw_input, expected", test_data)
def test_parse(raw_input, expected):
    assert_struct_type_equals(expected, StructType.parse(raw_input))


def test_parse_multiple():
    input_multiple = """
        first_schema = StructType([
          StructField('name12', LongType(), True),
          StructField('name22', DecimalType(3, 2), True),
          StructField('name32', ByteType(), False),
          StructField('name42', IntegerType(), True),
          StructField('name52', DateType(), True),
          StructField('name62', TimestampType(), True),
          StructField('name72', ShortType(), False),
        ])

      my_cool_schema     =StructType([StructField('name12',LongType(),False),
          StructField('name22',StringType(), True),StructField('name32',ByteType(), False),
          StructField('name42',IntegerType(), True),         StructField('name52',DateType(), True),
          StructField("name62",TimestampType(),True),
          StructField('name72',ShortType(),False)
        ])
    """

    expected = {
        'first_schema': StructType('first_schema', [
            StructField('name12', LongType(), True),
            StructField('name22', DecimalType(3, 2), True),
            StructField('name32', ByteType(), False),
            StructField('name42', IntegerType(), True),
            StructField('name52', DateType(), True),
            StructField('name62', TimestampType(), True),
            StructField('name72', ShortType(), False),
        ]),
        'my_cool_schema': StructType('my_cool_schema', [
            StructField('name12', LongType(), False),
            StructField('name22', StringType(), True),
            StructField('name32', ByteType(), False),
            StructField('name42', IntegerType(), True),
            StructField('name52', DateType(), True),
            StructField('name62', TimestampType(), True),
            StructField('name72', ShortType(), False),
        ])
    }

    actual = StructType.parse_multiple(input_multiple)
    assert_struct_type_equals(expected['first_schema'], actual['first_schema'])
    assert_struct_type_equals(expected['my_cool_schema'], actual['my_cool_schema'])
