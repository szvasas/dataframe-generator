import re


class StructField:
    def __init__(self, name: str, data_type: str, nullable: bool):
        self.name = name
        self.data_type = data_type
        self.nullable = nullable

    @staticmethod
    def parse(raw_string: str):
        trimmed_raw_string = raw_string.strip()
        match_result = re.match(r'StructField\((.*?), (.*?), (.*?)\)', trimmed_raw_string)
        return StructField(match_result.group(1), match_result.group(2), match_result.group(3))
