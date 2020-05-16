import re


class DataType:

    @staticmethod
    def type_descriptor() -> str:
        pass

    @staticmethod
    def parse(data_type, raw_string: str):
        if re.match(data_type.type_descriptor, raw_string):
            return data_type()
        else:
            return None


class ByteType(DataType):
    type_descriptor = r'ByteType\(\)'


class ShortType(DataType):
    type_descriptor = r'ShortType\(\)'


class IntegerType(DataType):
    type_descriptor = r'IntegerType\(\)'


class LongType(DataType):
    type_descriptor = r'LongType\(\)'


class DecimalType(DataType):
    type_descriptor = r'DecimalType\(\s*\d+\s*,\s*\d+\s*\)'


class StringType(DataType):
    type_descriptor = r'StringType\(\)'


class DateType(DataType):
    type_descriptor = r'DateType\(\)'


class TimestampType(DataType):
    type_descriptor = r'TimestampType\(\)'


supported_types = [ByteType,
                   ShortType,
                   IntegerType,
                   LongType,
                   DecimalType,
                   StringType,
                   DateType,
                   TimestampType
                   ]
