from dataframe_generator.main import StructType, StructField
from tests.matchers import assert_struct_type_equals

input = """
  schemaname2 = StructType([
    StructField('name12', LongType(), True),
    StructField('name22', StringType(), True),
    StructField('name32', ByteType(), False),
    StructField('name42', IntegerType(), True),
    StructField('name52', DateType(), True),
    StructField('name62', TimestampType(), True),
    StructField('name72', ShortyType(), False),
  ])
"""

expected = StructType('schemaname2', [
    StructField('name12', 'LongType()', True),
    StructField('name22', 'StringType()', True),
    StructField('name32', 'ByteType()', False),
    StructField('name42', 'IntegerType()', True),
    StructField('name52', 'DateType()', True),
    StructField('name62', 'TimestampType()', True),
    StructField('name72', 'ShortyType()', False),
])


def test_parse():
    assert_struct_type_equals(expected, StructType.parse(input))
