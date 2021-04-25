"""
Names a protocol buffer message, enumerated type, service, map, or a scalar. This class models a
fully-qualified name using the protocol buffer package.
"""

from karapace.protobuf.kotlin_wrapper import check, require
from karapace.protobuf.option_element import OptionElement


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

    """ Creates a scalar or message type.  """

    def __init__(self, is_scalar: bool, string: str, key_type=None, value_type=None):

        self.BOOL = ProtoType(True, "bool")
        self.BYTES = ProtoType(True, "bytes")
        self.DOUBLE = ProtoType(True, "double")
        self.FLOAT = ProtoType(True, "float")
        self.FIXED32 = ProtoType(True, "fixed32")
        self.FIXED64 = ProtoType(True, "fixed64")
        self.INT32 = ProtoType(True, "int32")
        self.INT64 = ProtoType(True, "int64")
        self.SFIXED32 = ProtoType(True, "sfixed32")
        self.SFIXED64 = ProtoType(True, "sfixed64")
        self.SINT32 = ProtoType(True, "sint32")
        self.SINT64 = ProtoType(True, "sint64")
        self.STRING = ProtoType(True, "string")
        self.UINT32 = ProtoType(True, "uint32")
        self.UINT64 = ProtoType(True, "uint64")
        self.ANY = ProtoType(False, "google.protobuf.Any")
        self.DURATION = ProtoType(False, "google.protobuf.Duration")
        self.TIMESTAMP = ProtoType(False, "google.protobuf.Timestamp")
        self.EMPTY = ProtoType(False, "google.protobuf.Empty")
        self.STRUCT_MAP = ProtoType(False, "google.protobuf.Struct")
        self.STRUCT_VALUE = ProtoType(False, "google.protobuf.Value")
        self.STRUCT_NULL = ProtoType(False, "google.protobuf.NullValue")
        self.STRUCT_LIST = ProtoType(False, "google.protobuf.ListValue")
        self.DOUBLE_VALUE = ProtoType(False, "google.protobuf.DoubleValue")
        self.FLOAT_VALUE = ProtoType(False, "google.protobuf.FloatValue")
        self.INT64_VALUE = ProtoType(False, "google.protobuf.Int64Value")
        self.UINT64_VALUE = ProtoType(False, "google.protobuf.UInt64Value")
        self.INT32_VALUE = ProtoType(False, "google.protobuf.Int32Value")
        self.UINT32_VALUE = ProtoType(False, "google.protobuf.UInt32Value")
        self.BOOL_VALUE = ProtoType(False, "google.protobuf.BoolValue")
        self.STRING_VALUE = ProtoType(False, "google.protobuf.StringValue")
        self.BYTES_VALUE = ProtoType(False, "google.protobuf.BytesValue")

        self.SCALAR_TYPES_ = [self.BOOL,
                              self.BYTES,
                              self.DOUBLE,
                              self.FLOAT,
                              self.FIXED32,
                              self.FIXED64,
                              self.INT32,
                              self.INT64,
                              self.SFIXED32,
                              self.SFIXED64,
                              self.SINT32,
                              self.SINT64,
                              self.STRING,
                              self.UINT32,
                              self.UINT64
                              ]

        self.SCALAR_TYPES: dict = dict()

        for a in self.SCALAR_TYPES_:
            self.SCALAR_TYPES[a.string] = a

        self.NUMERIC_SCALAR_TYPES: tuple = (
            self.DOUBLE,
            self.FLOAT,
            self.FIXED32,
            self.FIXED64,
            self.INT32,
            self.INT64,
            self.SFIXED32,
            self.SFIXED64,
            self.SINT32,
            self.SINT64,
            self.UINT32,
            self.UINT64
        )

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

    @staticmethod
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

    """ Returns the enclosing type, or null if self type is not nested in another type.  """

    @property
    def enclosing_type_or_package(self) -> str:
        dot = self.string.rfind(".")
        return None if (dot == -1) else self.string[:dot]

    """
     Returns a string like "type.googleapis.com/packagename.messagename" or null if self type is
     a scalar or a map. Note that self returns a non-null string for enums because it doesn't know
     if the named type is a message or an enum.
    """

    @property
    def type_url(self) -> str:
        return None if self.is_scalar or self.is_map else f"type.googleapis.com/{self.string}"

    def nested_type(self, name: str) -> object:  # ProtoType

        check(not self.is_scalar, "scalar cannot have a nested type")
        check(not self.is_map, "map cannot have a nested type")
        require(name and name.rfind(".") == -1 and len(name) != 0, f"unexpected name: {name}")

        return ProtoType(False, f"{self.string}.{name}")

    def __eq__(self, other):
        return type(other) is ProtoType and self.string == other.string

    def __ne__(self, other):
        return type(other) is not ProtoType or self.string != other.string

    def to_string(self) -> str:
        return self.string

    def hash_code(self) -> int:
        return hash(self.string)

    def get(self, enclosing_type_or_package: str, type_name: str) -> object:
        return self.get2(f"{enclosing_type_or_package}.{type_name}") if enclosing_type_or_package else self.get2(
            type_name)

    def get2(self, name: str) -> object:
        scalar = self.SCALAR_TYPES[name]
        if scalar:
            return scalar
        require(name and len(name) != 0 and name.rfind("#") == -1, f"unexpected name: {name}")
        if name.startswith("map<") and name.endswith(">"):
            comma = name.rfind(",")
            require(comma != -1, f"expected ',' in map type: {name}")
            key = self.get2(name[4:comma].strip())
            value = self.get2(name[comma + 1:len(name)].strip())
            return ProtoType(False, name, key, value)
        return ProtoType(False, name)

    @staticmethod
    def get3(key_type: object, value_type: object, name: str):
        return ProtoType(False, name, key_type, value_type)
