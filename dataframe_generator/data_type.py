import re
import string
from datetime import date, timedelta, datetime
from math import copysign
from random import randint, choice


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

    def next_value(self):
        pass


class ByteType(DataType):
    type_descriptor = r'ByteType\(\)'

    def next_value(self) -> int:
        return randint(-128, 127)


class ShortType(DataType):
    type_descriptor = r'ShortType\(\)'

    def next_value(self) -> int:
        return randint(-32768, 32767)


class IntegerType(DataType):
    type_descriptor = r'IntegerType\(\)'

    def next_value(self) -> int:
        return randint(-2147483648, 2147483647)


class LongType(DataType):
    type_descriptor = r'LongType\(\)'

    def next_value(self) -> int:
        return randint(-9223372036854775808, 9223372036854775807)


class DecimalType(DataType):
    type_descriptor = r'DecimalType\(\s*\d+\s*,\s*\d+\s*\)'
    type_descriptor_grouped = r'DecimalType\(\s*(\d+)\s*,\s*(\d+)\s*\)'

    @staticmethod
    def parse(data_type, raw_string: str):
        match_result = re.match(DecimalType.type_descriptor_grouped, raw_string)
        if match_result is None:
            return None
        else:
            scale = int(match_result.group(1).strip())
            precision = int(match_result.group(2).strip())
            return DecimalType(scale, precision)

    def __init__(self, scale: int, precision: int):
        self.scale = scale
        self.precision = precision

    def next_value(self) -> int:
        range_max = pow(10, self.scale) - 1
        unscaled = randint(0, range_max)
        signum = randint(-1, 0)
        return copysign(unscaled, signum) / pow(10, self.precision)


class StringType(DataType):
    type_descriptor = r'StringType\(\)'

    def next_value(self, length=10) -> str:
        letters = string.ascii_lowercase
        return ''.join((choice(letters) for i in range(length)))


class DateType(DataType):
    type_descriptor = r'DateType\(\)'

    def next_value(self) -> date:
        end = date.today()
        random_days = randint(0, 1000)
        return end - timedelta(days=random_days)


class TimestampType(DataType):
    type_descriptor = r'TimestampType\(\)'

    def next_value(self) -> datetime:
        end = datetime.now()
        random_seconds = randint(0, 100000000)
        return end - timedelta(seconds=random_seconds)


supported_types = [ByteType,
                   ShortType,
                   IntegerType,
                   LongType,
                   DecimalType,
                   StringType,
                   DateType,
                   TimestampType
                   ]
