import json
from typing import Dict

from dataframe_generator.generator import generate_values
from dataframe_generator.outputter import generate_csv
from dataframe_generator.struct_type import StructType


def read_struct_types() -> Dict[str, StructType]:
    print("Enter the StructType definition(s): ")
    lines = []
    line = " "
    while len(line) > 0:
        line = input()
        lines.append(line)
    input_raw = '\n'.join(lines)
    if not input_raw:
        return dict()
    return StructType.parse_multiple(input_raw)


def choose_struct(structs: Dict[str, StructType]) -> str:
    indexed_structs = dict()
    i = 1
    for struct in structs.items():
        indexed_structs[i] = struct
        i = i + 1

    if len(indexed_structs) == 1:
        return indexed_structs[1][0]

    for i in indexed_structs.items():
        print(f"{i[0]}: {i[1][0]}")
    chosen_index_raw = input("Choose a StructType (empty to exit): ")
    if not chosen_index_raw:
        return ''
    try:
        chosen_index = int(chosen_index_raw)
        return indexed_structs[chosen_index][0]
    except:
        print("Wrong index.")
        return choose_struct(structs)


def enter_generator_loop(struct_types: Dict[str, StructType]):
    while True:
        struct_name = choose_struct(struct_types)
        if not struct_name:
            break

        try:
            num_rows_raw = input("Number of records to generate (empty to exit): ")
            if not num_rows_raw:
                break
            num_rows = int(num_rows_raw)
            preset_values_raw = input("Enter preset values: ")
            if not preset_values_raw:
                preset_values_raw = "{}"
            preset_values = json.loads(preset_values_raw)
            result = generate_values(num_rows, struct_types[struct_name], preset_values)
            result_string = generate_csv(result)
            print()
            print(result_string)
        except:
            print("Error parsing input.")


def main():
    struct_types = read_struct_types()
    if struct_types:
        enter_generator_loop(struct_types)


main()
