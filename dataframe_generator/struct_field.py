import re

from dataframe_generator.data_type import DataType, supported_types


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
            potential_result = supported_type.create_from_string(supported_type, trimmed_raw_string)
            if potential_result is not None:
                return potential_result

        return None

    @staticmethod
    def __parse_nullable(raw_nullable: str) -> bool:
        return raw_nullable.strip() == 'True'
