from dataframe_generator.generator import generate_values
from dataframe_generator.outputter import generate_csv
from dataframe_generator.struct_type import StructType

input_raw = """
first_schema = StructType([
          StructField('name1', ByteType(), True),
          StructField('name2', ShortType(), True),
          StructField('name3', IntegerType(), True),
          StructField('name4', LongType(), True),
          StructField('name5', DecimalType(3, 2), True),
          StructField('name6', StringType(), False),
          StructField('name7', DateType(), True),
          StructField('name8', TimestampType(), True),
        ])
"""

struct = StructType.parse(input_raw)

result = generate_values(10, struct)

result_string = generate_csv(result)
print(result_string)
