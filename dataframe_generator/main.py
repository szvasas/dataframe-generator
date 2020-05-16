import random
from parse import *
import re


input = """
  schemaname = StructType([
    StructField('name1', LongType(), True),
    StructField('name2', StringType(), True),
    StructField('name3', ByteType(), True),
    StructField('name4', IntegerType(), True),
    StructField('name5', DateType(), True),
    StructField('name6', TimestampType(), True),
    StructField('name7', DecimalType(13, 2), True),
  ])

"""

input2 = "valami(12313) valami(45412) valami(1235)"

# result = re.match(".*?StructType(.*?)", input2)

result = re.findall(r'StructField\(.*?\).*?\)', input, re.DOTALL)

first_result = result[0]

result = re.match(r'StructField\((.*?), (.*?), (.*?)\)', first_result)

print(result.group())
print(result.group(1))
print(result.group(2))
print(result.group(3))
