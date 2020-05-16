import pytest

from dataframe_generator.generators import LongType, StringType, ByteType, IntegerType, DateType, TimestampType, \
    ShortType, DecimalType
from dataframe_generator.main import StructType, StructField
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

    expected = [
        StructType('first_schema', [
            StructField('name12', LongType(), True),
            StructField('name22', DecimalType(3, 2), True),
            StructField('name32', ByteType(), False),
            StructField('name42', IntegerType(), True),
            StructField('name52', DateType(), True),
            StructField('name62', TimestampType(), True),
            StructField('name72', ShortType(), False),
        ]),
        StructType('my_cool_schema', [
            StructField('name12', LongType(), False),
            StructField('name22', StringType(), True),
            StructField('name32', ByteType(), False),
            StructField('name42', IntegerType(), True),
            StructField('name52', DateType(), True),
            StructField('name62', TimestampType(), True),
            StructField('name72', ShortType(), False),
        ])
    ]

    actual = StructType.parse_multiple(input_multiple)
    assert_struct_type_equals(expected[0], actual[0])
    assert_struct_type_equals(expected[1], actual[1])
