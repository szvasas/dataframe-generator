from random import randint
from typing import List, Dict

from dataframe_generator.struct_field import StructField
from dataframe_generator.struct_type import StructType


def generate_values(num_rows: int, struct_type: StructType) -> Dict:
    values = []
    for _ in range(1, num_rows):
        row = []
        for field in struct_type.fields:
            if __will_it_be_none(field):
                row.append(None)
            else:
                row.append(field.data_type.next_value())
        values.append(row)

    return {
        'field_names': __create_field_name_list(struct_type),
        'values': values
    }


def __will_it_be_none(field: StructField) -> bool:
    return field.nullable and randint(1, 10) == 5


def __create_field_name_list(struct_type: StructType) -> List:
    return list(map(lambda field: field.name, struct_type.fields))
