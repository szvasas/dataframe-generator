class ByteGenerator:
    type_descriptor = r'ByteType\(\)'


class ShortGenerator:
    type_descriptor = r'ShortType\(\)'


class IntegerGenerator:
    type_descriptor = r'IntegerType\(\)'


class LongGenerator:
    type_descriptor = r'LongType\(\)'


class DecimalGenerator:
    type_descriptor = r'DecimalType\(\s*\d+\s*,\s*\d+\s*\)'


class StringGenerator:
    type_descriptor = r'StringType\(\)'


class DateGenerator:
    type_descriptor = r'DateType\(\)'


class TimestampGenerator:
    type_descriptor = r'TimestampType\(\)'


supported_generators = [ByteGenerator(),
                        ShortGenerator(),
                        IntegerGenerator(),
                        LongGenerator(),
                        DecimalGenerator(),
                        StringGenerator(),
                        DateGenerator(),
                        TimestampGenerator()
                        ]
