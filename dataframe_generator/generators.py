import re


class DataType:

    @staticmethod
    def type_descriptor() -> str:
        pass

    @staticmethod
    def parse(data_type, raw_string: str):
        if re.match(data_type.type_descriptor(), raw_string):
            return data_type()
        else:
            return None


class ByteType(DataType):

    @staticmethod
    def type_descriptor() -> str:
        return r'ByteType\(\)'


class ShortType(DataType):

    @staticmethod
    def type_descriptor() -> str:
        return r'ShortType\(\)'


class IntegerType(DataType):

    @staticmethod
    def type_descriptor() -> str:
        return r'IntegerType\(\)'


class LongType(DataType):

    @staticmethod
    def type_descriptor() -> str:
        return r'LongType\(\)'


class DecimalType(DataType):

    @staticmethod
    def type_descriptor() -> str:
        return r'DecimalType\(\s*\d+\s*,\s*\d+\s*\)'


class StringType(DataType):

    @staticmethod
    def type_descriptor() -> str:
        return r'StringType\(\)'


class DateType(DataType):

    @staticmethod
    def type_descriptor() -> str:
        return r'DateType\(\)'


class TimestampType(DataType):

    @staticmethod
    def type_descriptor() -> str:
        return r'TimestampType\(\)'


supported_types = [ByteType,
                   ShortType,
                   IntegerType,
                   LongType,
                   DecimalType,
                   StringType,
                   DateType,
                   TimestampType
                   ]
