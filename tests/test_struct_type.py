import pytest

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
          StructField('name72', ShortyType(), False),
        ])""",
        StructType('schemaname2', [
          StructField('name12', 'LongType()', True),
          StructField('name22', 'StringType()', True),
          StructField('name32', 'ByteType()', False),
          StructField('name42', 'IntegerType()', True),
          StructField('name52', 'DateType()', True),
          StructField('name62', 'TimestampType()', True),
          StructField('name72', 'ShortyType()', False),
        ])
     ),
]


input_multiple = """
  schemaname =         StructType([
    StructField('name1', LongType(), True),
    StructField('name2', StringType(), True),
    StructField('name3', ByteType(), True),
    StructField('name4', IntegerType(), True),
    StructField('name5', DateType(), True),
    StructField('name6', TimestampType(), True),
    StructField('name7', ShortType(), True),
  ])

  schemaname2 = StructType([
    StructField('name12', LongType(), True),
    StructField('name22', StringType(), True),
    StructField('name32', ByteType(), True),
    StructField('name42', IntegerType(), True),
    StructField('name52', DateType(), True),
    StructField('name62', TimestampType(), True),
    StructField('name72', DateType(), True),
  ])
"""


@pytest.mark.parametrize("raw_input, expected", test_data)
def test_parse(raw_input, expected):
    assert_struct_type_equals(expected, StructType.parse(raw_input))


def test_parse_multiple():
    result = StructType.parse_multiple(input_multiple)
    print(result)
