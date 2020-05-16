class DataType:

    def type_descriptor(self):
        pass


class ByteType(DataType):
    def type_descriptor(self):
        return r'ByteType\(\)'


class ShortType(DataType):
    def type_descriptor(self):
        return r'ShortType\(\)'


class IntegerType(DataType):
    def type_descriptor(self):
        return r'IntegerType\(\)'


class LongType(DataType):
    def type_descriptor(self):
        return r'LongType\(\)'


class DecimalType(DataType):
    def type_descriptor(self):
        return r'DecimalType\(\s*\d+\s*,\s*\d+\s*\)'


class StringType(DataType):
    def type_descriptor(self):
        return r'StringType\(\)'


class DateType(DataType):
    def type_descriptor(self):
        return r'DateType\(\)'


class TimestampType(DataType):
    def type_descriptor(self):
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
