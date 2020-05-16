class StructField:
    def __init__(self, name: str, data_type: str, nullable: bool):
        self.name = name
        self.data_type = data_type
        self.nullable = nullable
