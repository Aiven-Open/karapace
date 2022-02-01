from avro.schema import (
    ARRAY,
    ArraySchema,
    BOOLEAN,
    BYTES,
    DOUBLE,
    ENUM,
    EnumSchema,
    Field,
    FIXED,
    FixedSchema,
    FLOAT,
    INT,
    LONG,
    MAP,
    MapSchema,
    NamedSchema,
    Names,
    NULL,
    RECORD,
    RecordSchema,
    Schema,
    SchemaFromJSONData,
    STRING,
    UNION,
    UnionSchema,
)
from enum import Enum, unique
from typing import Any, cast, Dict, Generic, List, Optional, Set, TypeVar

import json

E = TypeVar("E", bound=Enum)


def parse_avro_schema_definition(s: str) -> Schema:
    """Compatibility function with Avro which ignores trailing data in JSON
    strings.

    The Python stdlib `json` module doesn't allow to ignore trailing data. If
    parsing fails because of it, the extra data can be removed and parsed
    again.
    """
    try:
        json_data = json.loads(s)
    except json.JSONDecodeError as e:
        if e.msg != "Extra data":
            raise

        json_data = json.loads(s[: e.pos])

    names = Names()
    return SchemaFromJSONData(json_data, names)


def is_incompatible(result: "SchemaCompatibilityResult") -> bool:
    return result.compatibility is SchemaCompatibilityType.incompatible


def is_compatible(result: "SchemaCompatibilityResult") -> bool:
    return result.compatibility is SchemaCompatibilityType.compatible


# TODO: remove SchemaCompatibilityType.incompatible, it can be determined from
# SchemaCompatibilityResult.incompatibilities
@unique
class SchemaCompatibilityType(Enum):
    compatible = "compatible"
    incompatible = "incompatible"
    recursion_in_progress = "recursion_in_progress"


@unique
class SchemaIncompatibilityType(Enum):
    name_mismatch = "name_mismatch"
    fixed_size_mismatch = "fixed_size_mismatch"
    missing_enum_symbols = "missing_enum_symbols"
    reader_field_missing_default_value = "reader_field_missing_default_value"
    type_mismatch = "type_mismatch"
    missing_union_branch = "missing_union_branch"


class AvroRuntimeException(Exception):
    pass


class SchemaCompatibilityResult(Generic[E]):
    def __init__(
        self,
        compatibility: SchemaCompatibilityType = SchemaCompatibilityType.recursion_in_progress,
        incompatibilities: Optional[List[E]] = None,
        messages: Optional[Set[str]] = None,
        locations: Optional[Set[str]] = None,
    ) -> None:
        self.compatibility = compatibility
        self.incompatibilities = incompatibilities or []
        self.messages = messages or set()
        self.locations = locations or set("/")  # Note: The empty set is replaced too

    def merged_with(self, that: "SchemaCompatibilityResult") -> "SchemaCompatibilityResult":
        """Returns a new instance with the results of `self` and `that` merged.

        This will not modified any object in-place. It will instantiate a new
        SchemaCompatibilityResult and return that.
        """

        # Ignore the messages and locations of `self` if it is a compatible object
        if self.compatibility is SchemaCompatibilityType.compatible:
            compat = that.compatibility
            messages = that.messages
            locations = that.locations
        else:
            compat = self.compatibility
            messages = self.messages.union(that.messages)
            locations = self.locations.union(that.locations)

        incompatibilities = self.incompatibilities + that.incompatibilities

        return SchemaCompatibilityResult(
            compatibility=compat, incompatibilities=incompatibilities, messages=messages, locations=locations
        )

    def add_incompatibility(self, incompat_type: E, message: str, location: List[str]) -> None:
        """Add an incompatibility, this will modify the object in-place."""
        formatted_location = "/".join(location[1:] if len(location) > 1 else location)

        self.compatibility = SchemaCompatibilityType.incompatible
        self.incompatibilities.append(incompat_type)
        self.messages.add(message)
        self.locations.add(formatted_location)

    @staticmethod
    def compatible() -> "SchemaCompatibilityResult":
        return SchemaCompatibilityResult(SchemaCompatibilityType.compatible)

    @staticmethod
    def incompatible(incompat_type: E, message: str, location: List[str]) -> "SchemaCompatibilityResult":
        locations = "/".join(location)
        if len(location) > 1:  # Remove ROOT_REFERENCE_TOKEN
            locations = locations[1:]
        return SchemaCompatibilityResult(
            compatibility=SchemaCompatibilityType.incompatible,
            incompatibilities=[incompat_type],
            locations={locations},
            messages={message},
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, SchemaCompatibilityResult):
            return False

        return (
            self.locations == other.locations
            and self.messages == other.messages
            and self.compatibility == other.compatibility
            and self.incompatibilities == other.incompatibilities
        )

    def __str__(self) -> str:
        return f"{self.compatibility}: {self.messages}"


class ReaderWriter:
    def __init__(self, reader: Schema, writer: Schema) -> None:
        self.reader, self.writer = reader, writer

    def __hash__(self) -> int:
        return id(self.reader) ^ id(self.writer)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ReaderWriter):
            return False
        return self.reader is other.reader and self.writer is other.writer


class ReaderWriterCompatibilityChecker:
    ROOT_REFERENCE_TOKEN = "/"

    def __init__(self) -> None:
        self.memoize_map: Dict[ReaderWriter, SchemaCompatibilityResult] = {}

    def get_compatibility(
        self,
        reader: Schema,
        writer: Schema,
        reference_token: str = ROOT_REFERENCE_TOKEN,
        location: Optional[List[str]] = None,
    ) -> SchemaCompatibilityResult:
        if location is None:
            location = []
        location.append(reference_token)
        pair = ReaderWriter(reader, writer)
        if pair in self.memoize_map:
            result = self.memoize_map[pair]
            if result.compatibility is SchemaCompatibilityType.recursion_in_progress:
                result = SchemaCompatibilityResult.compatible()
        else:
            self.memoize_map[pair] = SchemaCompatibilityResult()
            result = self.calculate_compatibility(reader, writer, location)
            self.memoize_map[pair] = result
        location.pop()
        return result

    # pylint: disable=too-many-return-statements
    def calculate_compatibility(
        self,
        reader: Schema,
        writer: Schema,
        location: List[str],
    ) -> SchemaCompatibilityResult:
        assert reader is not None
        assert writer is not None
        result = SchemaCompatibilityResult.compatible()
        if reader.type == writer.type:
            if reader.type in {NULL, BOOLEAN, INT, LONG, FLOAT, DOUBLE, BYTES, STRING}:
                return result
            if reader.type == ARRAY:
                reader, writer = cast(ArraySchema, reader), cast(ArraySchema, writer)
                return result.merged_with(self.get_compatibility(reader.items, writer.items, "items", location))
            if reader.type == MAP:
                reader, writer = cast(MapSchema, reader), cast(MapSchema, writer)
                return result.merged_with(self.get_compatibility(reader.values, writer.values, "values", location))
            if reader.type == FIXED:
                reader, writer = cast(FixedSchema, reader), cast(FixedSchema, writer)
                result = result.merged_with(self.check_schema_names(reader, writer, location))
                return result.merged_with(self.check_fixed_size(reader, writer, location))
            if reader.type == ENUM:
                reader, writer = cast(EnumSchema, reader), cast(EnumSchema, writer)
                result = result.merged_with(self.check_schema_names(reader, writer, location))
                return result.merged_with(self.check_reader_enum_contains_writer_enum(reader, writer, location))
            if reader.type == RECORD:
                reader, writer = cast(RecordSchema, reader), cast(RecordSchema, writer)
                result = result.merged_with(self.check_schema_names(reader, writer, location))
                return result.merged_with(self.check_reader_writer_record_fields(reader, writer, location))
            if reader.type == UNION:
                reader, writer = cast(UnionSchema, reader), cast(UnionSchema, writer)
                for i, writer_branch in enumerate(writer.schemas):
                    location.append(f"{i}")
                    compat = self.get_compatibility(reader, writer_branch)
                    if compat.compatibility is SchemaCompatibilityType.incompatible:
                        result = result.merged_with(
                            SchemaCompatibilityResult.incompatible(
                                SchemaIncompatibilityType.missing_union_branch,
                                f"reader union lacking writer type: {writer_branch.type.upper()}",
                                location,
                            )
                        )
                    location.pop()
                return result
            raise AvroRuntimeException(f"Unknown schema type: {reader.type}")
        if writer.type == UNION:
            writer = cast(UnionSchema, writer)
            for s in writer.schemas:
                result = result.merged_with(self.get_compatibility(reader, s))
            return result
        if reader.type in {NULL, BOOLEAN, INT}:
            return result.merged_with(self.type_mismatch(reader, writer, location))
        if reader.type == LONG:
            if writer.type == INT:
                return result
            return result.merged_with(self.type_mismatch(reader, writer, location))
        if reader.type == FLOAT:
            if writer.type in {INT, LONG}:
                return result
            return result.merged_with(self.type_mismatch(reader, writer, location))
        if reader.type == DOUBLE:
            if writer.type in {INT, LONG, FLOAT}:
                return result
            return result.merged_with(self.type_mismatch(reader, writer, location))
        if reader.type == BYTES:
            if writer.type == STRING:
                return result
            return result.merged_with(self.type_mismatch(reader, writer, location))
        if reader.type == STRING:
            if writer.type == BYTES:
                return result
            return result.merged_with(self.type_mismatch(reader, writer, location))
        if reader.type in {ARRAY, MAP, FIXED, ENUM, RECORD}:
            return result.merged_with(self.type_mismatch(reader, writer, location))
        if reader.type == UNION:
            reader = cast(UnionSchema, reader)
            for reader_branch in reader.schemas:
                compat = self.get_compatibility(reader_branch, writer)
                if compat.compatibility is SchemaCompatibilityType.compatible:
                    return result
            # No branch in reader compatible with writer
            message = f"reader union lacking writer type {writer.type}"
            return result.merged_with(
                SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.missing_union_branch, message, location)
            )
        raise AvroRuntimeException(f"Unknown schema type: {reader.type}")

    # pylint: enable=too-many-return-statements

    @staticmethod
    def check_schema_names(reader: NamedSchema, writer: NamedSchema, location: List[str]) -> SchemaCompatibilityResult:
        result = SchemaCompatibilityResult.compatible()
        location.append("name")
        if not ReaderWriterCompatibilityChecker.schema_name_equals(reader, writer):
            message = f"expected: {writer.fullname}"
            result = SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.name_mismatch, message, location)
        location.pop()
        return result

    @staticmethod
    def check_fixed_size(reader: FixedSchema, writer: FixedSchema, location: List[str]) -> SchemaCompatibilityResult:
        result = SchemaCompatibilityResult.compatible()
        location.append("size")
        actual = reader.size
        expected = writer.size
        if actual != expected:
            message = f"expected: {expected}, found: {actual}"
            result = SchemaCompatibilityResult.incompatible(
                SchemaIncompatibilityType.fixed_size_mismatch,
                message,
                location,
            )
        location.pop()
        return result

    @staticmethod
    def check_reader_enum_contains_writer_enum(
        reader: EnumSchema, writer: EnumSchema, location: List[str]
    ) -> SchemaCompatibilityResult:
        result = SchemaCompatibilityResult.compatible()
        location.append("symbols")
        writer_symbols, reader_symbols = set(writer.symbols), set(reader.symbols)
        extra_symbols = writer_symbols.difference(reader_symbols)
        if extra_symbols:
            default = reader.props.get("default")
            if default and default in reader_symbols:
                result = SchemaCompatibilityResult.compatible()
            else:
                result = SchemaCompatibilityResult.incompatible(
                    SchemaIncompatibilityType.missing_enum_symbols, f"{extra_symbols}", location
                )
        location.pop()
        return result

    @staticmethod
    def schema_name_equals(reader: NamedSchema, writer: NamedSchema) -> bool:
        if reader.name == writer.name:
            return True
        return writer.fullname in reader.props.get("aliases", [])

    @staticmethod
    def lookup_writer_field(writer_schema: RecordSchema, reader_field: Field) -> Optional[Field]:
        direct = writer_schema.field_map.get(reader_field.name)
        if direct:
            return direct
        for alias in reader_field.props.get("aliases", []):
            writer_field = writer_schema.field_map.get(alias)
            if writer_field is not None:
                return writer_field
        return None

    def check_reader_writer_record_fields(
        self, reader: RecordSchema, writer: RecordSchema, location: List[str]
    ) -> SchemaCompatibilityResult:
        result = SchemaCompatibilityResult.compatible()
        location.append("fields")
        for reader_field in reader.fields:
            reader_field = cast(Field, reader_field)
            location.append(f"{reader_field.index}")
            writer_field = self.lookup_writer_field(writer_schema=writer, reader_field=reader_field)
            if writer_field is None:
                if not reader_field.has_default:
                    if reader_field.type.type == ENUM and reader_field.type.props.get("default"):
                        result = result.merged_with(self.get_compatibility(reader_field.type, writer, "type", location))
                    else:
                        result = result.merged_with(
                            SchemaCompatibilityResult.incompatible(
                                SchemaIncompatibilityType.reader_field_missing_default_value, reader_field.name, location
                            )
                        )
            else:
                result = result.merged_with(
                    self.get_compatibility(reader_field.type, writer_field.type, "type", location),
                )
            location.pop()
        location.pop()
        return result

    @staticmethod
    def type_mismatch(reader: Schema, writer: Schema, location: List[str]) -> SchemaCompatibilityResult:
        message = f"reader type: {reader.type.upper()} not compatible with writer type: {writer.type.upper()}"
        return SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.type_mismatch, message, location)
