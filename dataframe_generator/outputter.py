import datetime
from typing import Dict


def generate_csv(values: Dict, separator: str = ',', header: bool = True) -> str:
    result = ''
    new_line = '\n'
    if header:
        result += separator.join(values['field_names']) + new_line
    for row in values['values']:
        for column in row:
            result += __to_string(column)
            result += separator
        result = result[:-1]
        result += new_line

    return result


def __to_string(column) -> str:
    if column is None:
        return ""
    elif isinstance(column, datetime.datetime):
        return column.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(column, datetime.date):
        return column.strftime("%Y-%m-%d")
    else:
        return str(column)
