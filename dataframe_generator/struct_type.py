import re
from typing import List, Dict

from dataframe_generator.struct_field import StructField


class StructType:
    def __init__(self, name: str, fields: List[StructField]):
        self.name = name
        self.fields = fields

    @staticmethod
    def parse_multiple(raw_string: str) -> Dict:
        trimmed_raw_string = raw_string.strip()
        raw_struct_type_strings = re.findall(r'.*?=.*?StructType\(\[.*?\]\)', trimmed_raw_string, re.DOTALL)
        parsed_struct_types = map(StructType.parse, raw_struct_type_strings)
        result = dict()
        for parsed in parsed_struct_types:
            result[parsed.name] = parsed

        return result

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
