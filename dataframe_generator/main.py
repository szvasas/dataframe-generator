import re
from random import randint
from typing import List

from dataframe_generator.data_type import supported_types, DataType


class StructField:
    def __init__(self, name: str, data_type: DataType, nullable: bool):
        self.name = name
        self.data_type = data_type
        self.nullable = nullable

    @staticmethod
    def parse(raw_string: str):
        trimmed_raw_string = raw_string.strip()
        data_types_regexp = "|".join(
            list(map(lambda supported_type: supported_type.type_descriptor, supported_types))
        )
        struct_field_template = r'StructField\((.*?),\s*(' + data_types_regexp + r')\s*,(.*?)\)'
        match_result = re.match(struct_field_template, trimmed_raw_string)

        name = StructField.__parse_name(match_result.group(1))
        data_type = StructField.__parse_data_type(match_result.group(2))
        nullable = StructField.__parse_nullable(match_result.group(3))
        return StructField(name, data_type, nullable)

    @staticmethod
    def __parse_name(raw_name: str) -> str:
        return raw_name.strip()[1:-1]

    @staticmethod
    def __parse_data_type(raw_string: str) -> DataType:
        trimmed_raw_string = raw_string.strip()
        for supported_type in supported_types:
            potential_result = supported_type.parse(supported_type, trimmed_raw_string)
            if potential_result is not None:
                return potential_result

        return None

    @staticmethod
    def __parse_nullable(raw_nullable: str) -> bool:
        return raw_nullable.strip() == 'True'


class StructType:
    def __init__(self, name: str, fields: List[StructField]):
        self.name = name
        self.fields = fields

    @staticmethod
    def parse_multiple(raw_string: str) -> List:
        trimmed_raw_string = raw_string.strip()
        raw_struct_type_strings = re.findall(r'.*?=.*?StructType\(\[.*?\]\)', trimmed_raw_string, re.DOTALL)
        return list(map(StructType.parse, raw_struct_type_strings))

    @staticmethod
    def parse(raw_string: str):
        trimmed_raw_string = raw_string.strip()
        match_result = re.match(r'(.*?)=.*?StructType\(\[(.*?)\]\)', trimmed_raw_string, re.DOTALL)
        name = StructType.__parse_name(match_result.group(1))
        fields = StructType.__parse_fields(match_result.group(2))

        return StructType(name, fields)

    @staticmethod
    def __parse_name(raw_name: str) -> str:
        return raw_name.strip()

    @staticmethod
    def __parse_fields(raw_fields: str) -> List[StructField]:
        trimmed_raw_fields = raw_fields.strip()
        struct_field_raw_strings = re.findall(r'StructField\(.*?\).*?\)', trimmed_raw_fields, re.DOTALL)
        return list(map(StructField.parse, struct_field_raw_strings))


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
