"""
Names a protocol buffer message, enumerated type, service, map, or a scalar. This class models a
fully-qualified name using the protocol buffer package.
"""

from karapace.protobuf.kotlin_wrapper import check, require
from karapace.protobuf.option_element import OptionElement


def static_init(cls):
    if getattr(cls, "static_init", None):
        cls.static_init()
    return cls


@static_init
class ProtoType:
    is_scalar: bool
    string: str
    is_map: bool
    """ The type of the map's keys. Only present when [is_map] is True.  """
    key_type: object  # ProtoType
    """ The type of the map's values. Only present when [is_map] is True.  """
    value_type: object  # ProtoType

    @property
    def simple_name(self) -> str:
        dot = self.string.rfind(".")
        return self.string[dot + 1]

    @classmethod
    def static_init(cls):
        cls.BOOL = ProtoType(True, "bool")
        cls.BYTES = ProtoType(True, "bytes")
        cls.DOUBLE = ProtoType(True, "double")
        cls.FLOAT = ProtoType(True, "float")
        cls.FIXED32 = ProtoType(True, "fixed32")
        cls.FIXED64 = ProtoType(True, "fixed64")
        cls.INT32 = ProtoType(True, "int32")
        cls.INT64 = ProtoType(True, "int64")
        cls.SFIXED32 = ProtoType(True, "sfixed32")
        cls.SFIXED64 = ProtoType(True, "sfixed64")
        cls.SINT32 = ProtoType(True, "sint32")
        cls.SINT64 = ProtoType(True, "sint64")
        cls.STRING = ProtoType(True, "string")
        cls.UINT32 = ProtoType(True, "uint32")
        cls.UINT64 = ProtoType(True, "uint64")
        cls.ANY = ProtoType(False, "google.protobuf.Any")
        cls.DURATION = ProtoType(False, "google.protobuf.Duration")
        cls.TIMESTAMP = ProtoType(False, "google.protobuf.Timestamp")
        cls.EMPTY = ProtoType(False, "google.protobuf.Empty")
        cls.STRUCT_MAP = ProtoType(False, "google.protobuf.Struct")
        cls.STRUCT_VALUE = ProtoType(False, "google.protobuf.Value")
        cls.STRUCT_NULL = ProtoType(False, "google.protobuf.NullValue")
        cls.STRUCT_LIST = ProtoType(False, "google.protobuf.ListValue")
        cls.DOUBLE_VALUE = ProtoType(False, "google.protobuf.DoubleValue")
        cls.FLOAT_VALUE = ProtoType(False, "google.protobuf.FloatValue")
        cls.INT64_VALUE = ProtoType(False, "google.protobuf.Int64Value")
        cls.UINT64_VALUE = ProtoType(False, "google.protobuf.UInt64Value")
        cls.INT32_VALUE = ProtoType(False, "google.protobuf.Int32Value")
        cls.UINT32_VALUE = ProtoType(False, "google.protobuf.UInt32Value")
        cls.BOOL_VALUE = ProtoType(False, "google.protobuf.BoolValue")
        cls.STRING_VALUE = ProtoType(False, "google.protobuf.StringValue")
        cls.BYTES_VALUE = ProtoType(False, "google.protobuf.BytesValue")

        cls.SCALAR_TYPES_ = [
            cls.BOOL, cls.BYTES, cls.DOUBLE, cls.FLOAT, cls.FIXED32, cls.FIXED64, cls.INT32, cls.INT64, cls.SFIXED32,
            cls.SFIXED64, cls.SINT32, cls.SINT64, cls.STRING, cls.UINT32, cls.UINT64
        ]

        cls.SCALAR_TYPES: dict = dict()

        for a in cls.SCALAR_TYPES_:
            cls.SCALAR_TYPES[a.string] = a

        cls.NUMERIC_SCALAR_TYPES: tuple = (
            cls.DOUBLE, cls.FLOAT, cls.FIXED32, cls.FIXED64, cls.INT32, cls.INT64, cls.SFIXED32, cls.SFIXED64, cls.SINT32,
            cls.SINT64, cls.UINT32, cls.UINT64
        )

    def __init__(self, is_scalar: bool, string: str, key_type=None, value_type=None):
        """ Creates a scalar or message type.  """
        if key_type is None and value_type is None:
            self.is_scalar = is_scalar
            self.string = string
            self.is_map = False
            self.key_type = None
            self.value_type = None
        else:
            if key_type.is_scalar() and key_type != self.BYTES and key_type != self.DOUBLE and key_type != self.FLOAT:
                self.is_scalar = False
                self.string = string
                self.is_map = True
                self.key_type = key_type  # TODO restrict what's allowed here
                self.value_type = value_type
            else:
                # TODO: must be IllegalArgumentException
                raise Exception("map key must be non-byte, non-floating point scalar: $key_type")

    def to_kind(self) -> OptionElement.Kind:
        return {
            "bool": OptionElement.Kind.BOOLEAN,
            "string": OptionElement.Kind.STRING,
            "bytes": OptionElement.Kind.STRING,
            "double": OptionElement.Kind.STRING,
            "float": OptionElement.Kind.STRING,
            "fixed32": OptionElement.Kind.STRING,
            "fixed64": OptionElement.Kind.STRING,
            "int32": OptionElement.Kind.STRING,
            "int64": OptionElement.Kind.STRING,
            "sfixed32": OptionElement.Kind.STRING,
            "sfixed64": OptionElement.Kind.STRING,
            "sint32": OptionElement.Kind.STRING,
            "sint64": OptionElement.Kind.STRING,
            "uint32": OptionElement.Kind.STRING,
            "uint64": OptionElement.Kind.NUMBER
        }.get(self.simple_name, OptionElement.Kind.ENUM)

    @property
    def enclosing_type_or_package(self) -> str:
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

    def nested_type(self, name: str) -> object:  # ProtoType

        check(not self.is_scalar, "scalar cannot have a nested type")
        check(not self.is_map, "map cannot have a nested type")
        require(name and name.rfind(".") == -1 and len(name) != 0, f"unexpected name: {name}")

        return ProtoType(False, f"{self.string}.{name}")

    def __eq__(self, other):
        return isinstance(other, ProtoType) and self.string == other.string

    def __ne__(self, other):
        return not isinstance(other, ProtoType) or self.string != other.string

    def __str__(self) -> str:
        return self.string

    def hash_code(self) -> int:
        return hash(self.string)

    @staticmethod
    def get(enclosing_type_or_package: str, type_name: str) -> object:
        return ProtoType.get2(f"{enclosing_type_or_package}.{type_name}") \
            if enclosing_type_or_package else ProtoType.get2(type_name)

    @staticmethod
    def get2(name: str):
        scalar = ProtoType.SCALAR_TYPES[name]
        if scalar:
            return scalar
        require(name and len(name) != 0 and name.rfind("#") == -1, f"unexpected name: {name}")
        if name.startswith("map<") and name.endswith(">"):
            comma = name.rfind(",")
            require(comma != -1, f"expected ',' in map type: {name}")
            key = ProtoType.get2(name[4:comma].strip())
            value = ProtoType.get2(name[comma + 1:len(name)].strip())
            return ProtoType(False, name, key, value)
        return ProtoType(False, name)

    @staticmethod
    def get3(key_type: object, value_type: object, name: str):
        return ProtoType(False, name, key_type, value_type)
