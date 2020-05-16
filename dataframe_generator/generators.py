class ByteType:
    type_descriptor = r'ByteType\(\)'


class ShortType:
    type_descriptor = r'ShortType\(\)'


class IntegerType:
    type_descriptor = r'IntegerType\(\)'


class LongType:
    type_descriptor = r'LongType\(\)'


class DecimalType:
    type_descriptor = r'DecimalType\(\s*\d+\s*,\s*\d+\s*\)'


class StringType:
    type_descriptor = r'StringType\(\)'


class DateType:
    type_descriptor = r'DateType\(\)'


class TimestampType:
    type_descriptor = r'TimestampType\(\)'


supported_generators = [ByteType(),
                        ShortType(),
                        IntegerType(),
                        LongType(),
                        DecimalType(),
                        StringType(),
                        DateType(),
                        TimestampType()
                        ]
