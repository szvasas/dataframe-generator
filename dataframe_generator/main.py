import re
from typing import List

from dataframe_generator.generators import supported_types, LongType, DataType


class StructField:
    def __init__(self, name: str, data_type: DataType, nullable: bool):
        self.name = name
        self.data_type = data_type
        self.nullable = nullable

    @staticmethod
    def parse(raw_string: str):
        trimmed_raw_string = raw_string.strip()
        data_types_regexp = "|".join(
            list(map(lambda supported_type: supported_type.type_descriptor(), supported_types))
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
    def __parse_data_type(raw_data_type: str) -> DataType:
        trimmed_raw_type = raw_data_type.strip()
        data_type = next(
            filter(lambda supported_type: re.match(supported_type.type_descriptor(), trimmed_raw_type), supported_types)
        )
        return data_type

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


def create_struct_type_raw_string_list(raw_string: str) -> List:
    return re.findall(r'.*?=.*?StructType\(\[.*?\]\)', raw_string, re.DOTALL)


def parse_struct_type_raw_string(raw_string: str):
    trimmed_input = raw_string.strip()
    result = re.match(r'(.*?)=.*?StructType\(\[(.*?)\]\)', trimmed_input, re.DOTALL)
    schema_name = result.group(1).strip()
    raw_struct_fields = result.group(2).strip()
    return schema_name, raw_struct_fields
