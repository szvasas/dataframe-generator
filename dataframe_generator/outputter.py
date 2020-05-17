from typing import Dict


def generate_csv(values: Dict, separator: str = ',', header: bool = True) -> str:
    result = ''
    new_line = '\n'
    if header:
        result += separator.join(values['field_names']) + new_line
    for row in values['values']:
        for column in row:
            result += str(column)
            result += separator
        result = result[:-1]
        result += new_line

    return result
