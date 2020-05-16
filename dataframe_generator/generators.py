import re


class DataType:

    def type_descriptor(self) -> str:
        pass

    def is_it_this_type(self, raw_string: str) -> bool:
        return re.match(self.type_descriptor(), raw_string) is not None


class ByteType(DataType):
    def type_descriptor(self) -> str:
        return r'ByteType\(\)'


class ShortType(DataType):
    def type_descriptor(self) -> str:
        return r'ShortType\(\)'


class IntegerType(DataType):
    def type_descriptor(self) -> str:
        return r'IntegerType\(\)'


class LongType(DataType):
    def type_descriptor(self) -> str:
        return r'LongType\(\)'


class DecimalType(DataType):
    def type_descriptor(self) -> str:
        return r'DecimalType\(\s*\d+\s*,\s*\d+\s*\)'


class StringType(DataType):
    def type_descriptor(self) -> str:
        return r'StringType\(\)'


class DateType(DataType):
    def type_descriptor(self) -> str:
        return r'DateType\(\)'


class TimestampType(DataType):
    def type_descriptor(self) -> str:
        return r'TimestampType\(\)'


supported_types = [ByteType(),
                   ShortType(),
                   IntegerType(),
                   LongType(),
                   DecimalType(),
                   StringType(),
                   DateType(),
                   TimestampType()
                   ]
