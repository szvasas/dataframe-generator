import re
from random import randint
from typing import List

from dataframe_generator.struct_type import StructType


def value_generator(num_rows: int, struct_type: StructType) -> List:
    result = []
    for _ in range(1, num_rows):
        row = []
        for field in struct_type.fields:
            is_none = field.nullable and randint(1, 10) == 5
            if is_none:
                row.append(None)
            else:
                row.append(field.data_type.next_value())
        result.append(row)

    return result
