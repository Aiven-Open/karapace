"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from .introspect import record_schema
from dataclasses import asdict, fields, is_dataclass
from enum import Enum
from functools import lru_cache, partial
from typing import Callable, cast, IO, Iterable, Mapping, TYPE_CHECKING, TypeVar, Union
from typing_extensions import get_args, get_origin, Self

import avro
import avro.io
import avro.schema
import uuid

if TYPE_CHECKING:
    from _typeshed import DataclassInstance
else:

    class DataclassInstance:
        ...


__all__ = ("AvroModel",)


Parser = Callable[[object], object]


def as_avro_value(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    # The avro library is a bit inconsistent as it doesn't handle uuid.UUID, but it
    # handles datetime instances.
    if isinstance(value, uuid.UUID):
        return value.hex
    # Again, the avro library is being overly strict and only accepting exactly list for
    # array types, instead of any sequence type.
    if isinstance(value, tuple):
        return list(value)
    return value


def avro_dict_factory(items: Iterable[tuple[str, object]]) -> dict:
    return {key: as_avro_value(value) for key, value in items}


def as_avro_dict(instance: DataclassInstance) -> dict[str, object]:
    return asdict(instance, dict_factory=avro_dict_factory)


def noop(x: object) -> object:
    return x


def from_avro_array(
    transformation: Parser,
    values: Iterable[object],
) -> tuple[object, ...]:
    return tuple(transformation(value) for value in values)


def optional_parser(parser: Parser | None) -> Parser | None:
    if parser is None:
        return None

    def parse(value: object) -> object:
        return None if value is None else parser(value)  # type: ignore[misc]

    return parse


def from_avro_value(type_: object) -> Parser | None:
    # pylint: disable=too-many-return-statements

    if isinstance(type_, type):
        if is_dataclass(type_):
            return partial(from_avro_dict, type_)
        if issubclass(type_, Enum):
            return type_
        # With the avro library we need to manually instantiate UUID.
        if issubclass(type_, uuid.UUID):
            return cast(Parser, uuid.UUID)

    origin = get_origin(type_)

    if origin is tuple:
        inner_type, ellipsis = get_args(type_)
        assert ellipsis is Ellipsis
        inner_transformation = from_avro_value(inner_type)
        return (
            tuple  # type: ignore[return-value]
            if inner_transformation is None
            else partial(from_avro_array, inner_transformation)
        )

    # With the avro library we need to manually handle union types. We only support the
    # special case of nullable types for now.
    if origin is Union:
        try:
            a, b = get_args(type_)
        except ValueError:
            raise NotImplementedError("Cannot handle arbitrary union types") from None
        if a is type(None):  # noqa: E721
            return optional_parser(from_avro_value(b))
        if b is type(None):  # noqa: E721
            return optional_parser(from_avro_value(a))
        raise NotImplementedError("Cannot handle arbitrary union types")

    return None


@lru_cache
def parser_transformations(cls: type[DataclassInstance]) -> Mapping[str, Parser]:
    cls_transformations = {}
    for field in fields(cls):
        transformation = from_avro_value(field.type)
        if transformation is not None:
            cls_transformations[field.name] = transformation
    return cls_transformations


T = TypeVar("T", bound=DataclassInstance)


def from_avro_dict(cls: type[T], data: Mapping[str, object]) -> T:
    cls_transformations = parser_transformations(cls)
    return cls(**{key: cls_transformations.get(key, noop)(value) for key, value in data.items()})


@lru_cache
def record_avsc_object(record_type: type[DataclassInstance]) -> avro.schema.Schema:
    return avro.schema.make_avsc_object(record_schema(record_type))


@lru_cache
def record_writer(record_type: type[DataclassInstance]) -> avro.io.DatumWriter:
    return avro.io.DatumWriter(record_avsc_object(record_type))


@lru_cache
def record_reader(record_type: type[DataclassInstance]) -> avro.io.DatumReader:
    return avro.io.DatumReader(record_avsc_object(record_type))


class AvroModel(DataclassInstance):
    def serialize(self, buffer: IO[bytes]) -> None:
        writer = record_writer(type(self))
        encoder = avro.io.BinaryEncoder(buffer)
        writer.write(as_avro_dict(self), encoder)

    @classmethod
    def parse(cls, buffer: IO[bytes]) -> Self:
        reader = record_reader(cls)
        decoder = avro.io.BinaryDecoder(buffer)
        decoded = reader.read(decoder)
        return from_avro_dict(cls, decoded)
