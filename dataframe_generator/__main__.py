import sys

from dataframe_generator.generator import generate_values
from dataframe_generator.outputter import generate_csv
from dataframe_generator.struct_type import StructType
import json

print("Enter the StructType definition: ")
lines = []
line = " "
while len(line) > 0:
    line = input()
    lines.append(line)
input_raw = '\n'.join(lines)

struct = StructType.parse(input_raw)


while True:
    num_rows_raw = input("Number of records to generate (empty to exit): ")
    if not num_rows_raw:
        break

    preset_values_raw = input("Enter preset values: ")
    if not preset_values_raw:
        preset_values_raw = "{}"

    try:
        num_rows = int(num_rows_raw)
        preset_values = json.loads(preset_values_raw)
        result = generate_values(num_rows, struct, preset_values)
        result_string = generate_csv(result)
        print(result_string)
    except:
        print("Error parsing input.")
