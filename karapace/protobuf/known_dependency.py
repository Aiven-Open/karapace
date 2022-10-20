# Support of known dependencies

from enum import Enum
from typing import Any, Dict, Set


def static_init(cls: Any) -> object:
    if getattr(cls, "static_init", None):
        cls.static_init()
    return cls


class KnownDependencyLocation(Enum):
    ANY_LOCATION = "google/protobuf/any.proto"
    API_LOCATION = "google/protobuf/api.proto"
    DESCRIPTOR_LOCATION = "google/protobuf/descriptor.proto"
    DURATION_LOCATION = "google/protobuf/duration.proto"
    EMPTY_LOCATION = "google/protobuf/empty.proto"
    FIELD_MASK_LOCATION = "google/protobuf/field_mask.proto"
    SOURCE_CONTEXT_LOCATION = "google/protobuf/source_context.proto"
    STRUCT_LOCATION = "google/protobuf/struct.proto"
    TIMESTAMP_LOCATION = "google/protobuf/timestamp.proto"
    TYPE_LOCATION = "google/protobuf/type.proto"
    WRAPPER_LOCATION = "google/protobuf/wrappers.proto"
    CALENDAR_PERIOD_LOCATION = "google/type/calendar_period.proto"
    COLOR_LOCATION = "google/type/color.proto"
    DATE_LOCATION = "google/type/date.proto"
    DATETIME_LOCATION = "google/type/datetime.proto"
    DAY_OF_WEEK_LOCATION = "google/type/dayofweek.proto"
    DECIMAL_LOCATION = "google/type/decimal.proto"
    EXPR_LOCATION = "google/type/expr.proto"
    FRACTION_LOCATION = "google/type/fraction.proto"
    INTERVAL_LOCATION = "google/type/interval.proto"
    LATLNG_LOCATION = "google/type/latlng.proto"
    MONEY_LOCATION = "google/type/money.proto"
    MONTH_LOCATION = "google/type/month.proto"
    PHONE_NUMBER_LOCATION = "google/type/phone_number.proto"
    POSTAL_ADDRESS_LOCATION = "google/type/postal_address.proto"
    QUATERNION_LOCATION = "google/type/quaternion.proto"
    TIME_OF_DAY_LOCATION = "google/type/timeofday.proto"


@static_init
class KnownDependency:
    index: Dict = dict()
    index_simple: Dict = dict()
    map: Dict = {
        "google/protobuf/any.proto": ["google.protobuf.Any"],
        "google/protobuf/api.proto": ["google.protobuf.Api", "google.protobuf.Method", "google.protobuf.Mixin"],
        "google/protobuf/descriptor.proto": [
            "google.protobuf.FileDescriptorSet",
            "google.protobuf.FileDescriptorProto",
            "google.protobuf.DescriptorProto",
            "google.protobuf.ExtensionRangeOptions",
            "google.protobuf.FieldDescriptorProto",
            "google.protobuf.OneofDescriptorProto",
            "google.protobuf.EnumDescriptorProto",
            "google.protobuf.EnumValueDescriptorProto",
            "google.protobuf.ServiceDescriptorProto",
            "google.protobuf.MethodDescriptorProto",
            "google.protobuf.FileOptions",
            "google.protobuf.MessageOptions",
            "google.protobuf.FieldOptions",
            "google.protobuf.OneofOptions",
            "google.protobuf.EnumOptions",
            "google.protobuf.EnumValueOptions",
            "google.protobuf.ServiceOptions",
            "google.protobuf.MethodOptions",
            "google.protobuf.UninterpretedOption",
            "google.protobuf.SourceCodeInfo",
            "google.protobuf.GeneratedCodeInfo",
        ],
        "google/protobuf/duration.proto": ["google.protobuf.Duration"],
        "google/protobuf/empty.proto": ["google.protobuf.Empty"],
        "google/protobuf/field_mask.proto": ["google.protobuf.FieldMask"],
        "google/protobuf/source_context.proto": ["google.protobuf.SourceContext"],
        "google/protobuf/struct.proto": [
            "google.protobuf.Struct",
            "google.protobuf.Value",
            "google.protobuf.NullValue",
            "google.protobuf.ListValue",
        ],
        "google/protobuf/timestamp.proto": ["google.protobuf.Timestamp"],
        "google/protobuf/type.proto": [
            "google.protobuf.Type",
            "google.protobuf.Field",
            "google.protobuf.Enum",
            "google.protobuf.EnumValue",
            "google.protobuf.Option",
            "google.protobuf.Syntax",
        ],
        "google/protobuf/wrappers.proto": [
            "google.protobuf.DoubleValue",
            "google.protobuf.FloatValue",
            "google.protobuf.Int64Value",
            "google.protobuf.UInt64Value",
            "google.protobuf.Int32Value",
            "google.protobuf.UInt32Value",
            "google.protobuf.BoolValue",
            "google.protobuf.StringValue",
            "google.protobuf.BytesValue",
        ],
        "google/type/calendar_period.proto": ["google.type.CalendarPeriod"],
        "google/type/color.proto": ["google.type.Color"],
        "google/type/date.proto": ["google.type.Date"],
        "google/type/datetime.proto": ["google.type.DateTime", "google.type.TimeZone"],
        "google/type/dayofweek.proto": ["google.type.DayOfWeek"],
        "google/type/decimal.proto": ["google.type.Decimal"],
        "google/type/expr.proto": ["google.type.Expr"],
        "google/type/fraction.proto": ["google.type.Fraction"],
        "google/type/interval.proto": ["google.type.Interval"],
        "google/type/latlng.proto": ["google.type.LatLng"],
        "google/type/money.proto": ["google.type.Money"],
        "google/type/month.proto": ["google.type.Month"],
        "google/type/phone_number.proto": ["google.type.PhoneNumber"],
        "google/type/postal_address.proto": ["google.type.PostalAddress"],
        "google/type/quaternion.proto": ["google.type.Quaternion"],
        "google/type/timeofday.proto": ["google.type.TimeOfDay"],
        "confluent/meta.proto": [".confluent.Meta"],
        "confluent/type/decimal.proto": [".confluent.type.Decimal"],
    }

    @classmethod
    def static_init(cls) -> None:
        for key, value in cls.map.items():
            for item in value:
                cls.index[item] = key
                dot = item.rfind(".")
                cls.index_simple[item[dot + 1 :]] = key


@static_init
class DependenciesHardcoded:
    index: Set = set()

    @classmethod
    def static_init(cls) -> None:
        cls.index = {
            "bool",
            "bytes",
            "double",
            "float",
            "fixed32",
            "fixed64",
            "int32",
            "int64",
            "sfixed32",
            "sfixed64",
            "sint32",
            "sint64",
            "string",
            "uint32",
            "uint64",
        }
