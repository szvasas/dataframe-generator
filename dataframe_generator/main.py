import random
from parse import *
import re
from typing import List

from dataframe_generator.StructField import StructField


def create_struct_type_raw_string_list(raw_string: str) -> List:
    return re.findall(r'.*?=.*?StructType\(\[.*?\]\)', raw_string, re.DOTALL)


def parse_struct_type_raw_string(raw_string: str):
    trimmed_input = raw_string.strip()
    result = re.match(r'(.*?)=.*?StructType\(\[(.*?)\]\)', trimmed_input, re.DOTALL)
    schema_name = result.group(1).strip()
    raw_struct_fields = result.group(2).strip()
    return schema_name, raw_struct_fields


input = """
  schemaname =         StructType([
    StructField('name1', LongType(), True),
    StructField('name2', StringType(), True),
    StructField('name3', ByteType(), True),
    StructField('name4', IntegerType(), True),
    StructField('name5', DateType(), True),
    StructField('name6', TimestampType(), True),
    StructField('name7', DecimalType(13, 2), True),
  ])

  schemaname2 = StructType([
    StructField('name12', LongType(), True),
    StructField('name22', StringType(), True),
    StructField('name32', ByteType(), True),
    StructField('name42', IntegerType(), True),
    StructField('name52', DateType(), True),
    StructField('name62', TimestampType(), True),
    StructField('name72', DecimalType(13, 2), True),
  ])
"""

# result = create_struct_type_raw_string_list(input)
# schema_name, raw_struct_fields = parse_struct_type_raw_string(result[0])
# print("success")

x = StructField('name', 'IntegerType()', True)
print(x)

# input2 = "valami(12313) valami(45412) valami(1235)"
#
# # result = re.match(".*?StructType(.*?)", input2)
#
# result = re.findall(r'StructField\(.*?\).*?\)', input, re.DOTALL)
#
# first_result = result[0]
#
# result = re.match(r'StructField\((.*?), (.*?), (.*?)\)', first_result)
#
# print(result.group())
# print(result.group(1))
# print(result.group(2))
# print(result.group(3))
