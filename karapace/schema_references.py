from karapace.schema_type import SchemaType
from karapace.typing import JsonData
from karapace.utils import json_encode
from typing import Any


class References:
    def __init__(self, schema_type: SchemaType, references: JsonData):
        """Schema with type information

        Args:
            schema_type (SchemaType): The type of the schema
            references (str): The original schema string
        """
        self.schema_type = schema_type
        self.references = references

    def val(self) -> JsonData:
        return self.references

    def json(self) -> str:
        return str(json_encode(self.references, sort_keys=True))

    def __eq__(self, other: Any) -> bool:
        if other is None or not isinstance(other, References):
            return False
        return self.json() == other.json()
