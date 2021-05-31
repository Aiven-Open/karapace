# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/Schema.kt
# Ported partially for required functionality.


class ProtobufSchema:
    schema: str

    def __init__(self, schema: str):
        self.schema = schema

    def __str__(self) -> str:
        return self.schema

    def to_json(self):
        return self.schema
