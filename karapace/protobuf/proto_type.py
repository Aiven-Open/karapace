# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/ProtoType.kt
"""
Names a protocol buffer message, enumerated type, service, map, or a scalar. This class models a
fully-qualified name using the protocol buffer package.
"""
from enum import auto, Enum
from karapace.protobuf.exception import IllegalArgumentException
from karapace.protobuf.kotlin_wrapper import check, require
from karapace.protobuf.option_element import OptionElement
from typing import Optional


def static_init(cls) -> object:
    if getattr(cls, "static_init", None):
        cls.static_init()
    return cls


@static_init
class ProtoType:
    @property
    def simple_name(self) -> str:
        dot = self.string.rfind(".")
        return self.string[dot + 1:]

    @classmethod
    def static_init(cls) -> None:
        cls.BOOL = cls(True, "bool")
        cls.BYTES = cls(True, "bytes")
        cls.DOUBLE = cls(True, "double")
        cls.FLOAT = cls(True, "float")
        cls.FIXED32 = cls(True, "fixed32")
        cls.FIXED64 = cls(True, "fixed64")
        cls.INT32 = cls(True, "int32")
        cls.INT64 = cls(True, "int64")
        cls.SFIXED32 = cls(True, "sfixed32")
        cls.SFIXED64 = cls(True, "sfixed64")
        cls.SINT32 = cls(True, "sint32")
        cls.SINT64 = cls(True, "sint64")
        cls.STRING = cls(True, "string")
        cls.UINT32 = cls(True, "uint32")
        cls.UINT64 = cls(True, "uint64")
        cls.ANY = cls(False, "google.protobuf.Any")
        cls.DURATION = cls(False, "google.protobuf.Duration")
        cls.TIMESTAMP = cls(False, "google.protobuf.Timestamp")
        cls.EMPTY = cls(False, "google.protobuf.Empty")
        cls.STRUCT_MAP = cls(False, "google.protobuf.Struct")
        cls.STRUCT_VALUE = cls(False, "google.protobuf.Value")
        cls.STRUCT_NULL = cls(False, "google.protobuf.NullValue")
        cls.STRUCT_LIST = cls(False, "google.protobuf.ListValue")
        cls.DOUBLE_VALUE = cls(False, "google.protobuf.DoubleValue")
        cls.FLOAT_VALUE = cls(False, "google.protobuf.FloatValue")
        cls.INT64_VALUE = cls(False, "google.protobuf.Int64Value")
        cls.UINT64_VALUE = cls(False, "google.protobuf.UInt64Value")
        cls.INT32_VALUE = cls(False, "google.protobuf.Int32Value")
        cls.UINT32_VALUE = cls(False, "google.protobuf.UInt32Value")
        cls.BOOL_VALUE = cls(False, "google.protobuf.BoolValue")
        cls.STRING_VALUE = cls(False, "google.protobuf.StringValue")
        cls.BYTES_VALUE = cls(False, "google.protobuf.BytesValue")

        cls.SCALAR_TYPES_ = [
            cls.BOOL, cls.BYTES, cls.DOUBLE, cls.FLOAT, cls.FIXED32, cls.FIXED64, cls.INT32, cls.INT64, cls.SFIXED32,
            cls.SFIXED64, cls.SINT32, cls.SINT64, cls.STRING, cls.UINT32, cls.UINT64
        ]

        cls.SCALAR_TYPES = {}

        for a in cls.SCALAR_TYPES_:
            cls.SCALAR_TYPES[a.string] = a

        cls.NUMERIC_SCALAR_TYPES: tuple = (
            cls.DOUBLE, cls.FLOAT, cls.FIXED32, cls.FIXED64, cls.INT32, cls.INT64, cls.SFIXED32, cls.SFIXED64, cls.SINT32,
            cls.SINT64, cls.UINT32, cls.UINT64
        )

    def __init__(
        self, is_scalar: bool, string: str, key_type: Optional['ProtoType'] = None, value_type: Optional['ProtoType'] = None
    ) -> None:
        """ Creates a scalar or message type.  """
        if not key_type and not value_type:
            self.is_scalar = is_scalar
            self.string = string
            self.is_map = False
            """ The type of the map's keys. Only present when [is_map] is True.  """
            self.key_type = None
            """ The type of the map's values. Only present when [is_map] is True.  """
            self.value_type = None
        else:
            if key_type.is_scalar and key_type != self.BYTES and key_type != self.DOUBLE and key_type != self.FLOAT:
                self.is_scalar = False
                self.string = string
                self.is_map = True
                self.key_type = key_type  # TODO restrict what's allowed here
                self.value_type = value_type
            else:
                # TODO: must be IllegalArgumentException
                raise Exception(f"map key must be non-byte, non-floating point scalar: {key_type}")

    def to_kind(self) -> OptionElement.Kind:
        return {
            "bool": OptionElement.Kind.BOOLEAN,
            "string": OptionElement.Kind.STRING,
            "bytes": OptionElement.Kind.NUMBER,
            "double": OptionElement.Kind.NUMBER,
            "float": OptionElement.Kind.NUMBER,
            "fixed32": OptionElement.Kind.NUMBER,
            "fixed64": OptionElement.Kind.NUMBER,
            "int32": OptionElement.Kind.NUMBER,
            "int64": OptionElement.Kind.NUMBER,
            "sfixed32": OptionElement.Kind.NUMBER,
            "sfixed64": OptionElement.Kind.NUMBER,
            "sint32": OptionElement.Kind.NUMBER,
            "sint64": OptionElement.Kind.NUMBER,
            "uint32": OptionElement.Kind.NUMBER,
            "uint64": OptionElement.Kind.NUMBER
        }.get(self.simple_name, OptionElement.Kind.ENUM)

    @property
    def enclosing_type_or_package(self) -> Optional[str]:
        """ Returns the enclosing type, or null if self type is not nested in another type.  """
        dot = self.string.rfind(".")
        return None if (dot == -1) else self.string[:dot]

    @property
    def type_url(self) -> str:
        """ Returns a string like "type.googleapis.com/packagename.messagename" or null if self type is
        a scalar or a map. Note that self returns a non-null string for enums because it doesn't know
        if the named type is a message or an enum.
        """
        return None if self.is_scalar or self.is_map else f"type.googleapis.com/{self.string}"

    def nested_type(self, name: str) -> 'ProtoType':

        check(not self.is_scalar, "scalar cannot have a nested type")
        check(not self.is_map, "map cannot have a nested type")
        require(name and name.rfind(".") == -1 and len(name) != 0, f"unexpected name: {name}")

        return ProtoType(False, f"{self.string}.{name}")

    def __eq__(self, other) -> bool:
        return isinstance(other, ProtoType) and self.string == other.string

    def __ne__(self, other) -> bool:
        return not isinstance(other, ProtoType) or self.string != other.string

    def __str__(self) -> str:
        return self.string

    def hash_code(self) -> int:
        return hash(self.string)

    @staticmethod
    def get(enclosing_type_or_package: str, type_name: str) -> 'ProtoType':
        return ProtoType.get2(f"{enclosing_type_or_package}.{type_name}") \
            if enclosing_type_or_package else ProtoType.get2(type_name)

    @staticmethod
    def get2(name: str) -> 'ProtoType':
        scalar = ProtoType.SCALAR_TYPES.get(name)
        if scalar:
            return scalar
        require(name and len(name) != 0 and name.rfind("#") == -1, f"unexpected name: {name}")
        if name.startswith("map<") and name.endswith(">"):
            comma = name.rfind(",")
            require(comma != -1, f"expected ',' in map type: {name}")
            key = ProtoType.get2(name[4:comma].strip())
            value = ProtoType.get2(name[comma + 1:len(name) - 1].strip())
            return ProtoType(False, name, key, value)
        return ProtoType(False, name)

    @staticmethod
    def get3(key_type: 'ProtoType', value_type: 'ProtoType', name: str) -> 'ProtoType':
        return ProtoType(False, name, key_type, value_type)

    # schema compatibility check functionality karapace addon
    # Based on table  https://developers.google.com/protocol-buffers/docs/proto3#scalar """

    class CompatibilityKind(Enum):
        VARIANT = auto()
        SVARIANT = auto()  # sint has incompatible format with int but compatible with it by size
        FIXED64 = auto()
        LENGTH_DELIMITED = auto()
        FIXED32 = auto()
        DOUBLE = auto()
        FLOAT = auto()

    def compatibility_kind(self, is_enum: bool) -> 'ProtoType.CompatibilityKind':
        if is_enum:
            return ProtoType.CompatibilityKind.VARIANT

        result = {
            "int32": ProtoType.CompatibilityKind.VARIANT,
            "int64": ProtoType.CompatibilityKind.VARIANT,
            "uint32": ProtoType.CompatibilityKind.VARIANT,
            "uint64": ProtoType.CompatibilityKind.VARIANT,
            "bool": ProtoType.CompatibilityKind.VARIANT,
            "sint32": ProtoType.CompatibilityKind.SVARIANT,
            "sint64": ProtoType.CompatibilityKind.SVARIANT,
            "double": ProtoType.CompatibilityKind.DOUBLE,  # it is compatible by size with FIXED64
            "fixed64": ProtoType.CompatibilityKind.FIXED64,
            "sfixed64": ProtoType.CompatibilityKind.FIXED64,
            "float": ProtoType.CompatibilityKind.FLOAT,  # it is compatible by size with FIXED32
            "fixed32": ProtoType.CompatibilityKind.FIXED32,
            "sfixed32": ProtoType.CompatibilityKind.FIXED32,
            "string": ProtoType.CompatibilityKind.LENGTH_DELIMITED,
            "bytes": ProtoType.CompatibilityKind.LENGTH_DELIMITED,
        }.get(self.simple_name)

        if result:
            return result

        raise IllegalArgumentException(f"undefined type: {self.simple_name}")
