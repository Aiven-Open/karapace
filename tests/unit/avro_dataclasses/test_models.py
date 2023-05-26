"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from dataclasses import dataclass, field
from karapace.avro_dataclasses.models import AvroModel
from typing import List, Optional, Tuple

import datetime
import enum
import io
import pytest
import uuid


class Symbol(enum.Enum):
    a = "a"
    b = "b"


@dataclass(frozen=True)
class NestedModel:
    bool_field: bool
    values: Tuple[int, ...]


@dataclass(frozen=True)
class RecordModel(AvroModel):
    symbol: Symbol
    height: int = field(metadata={"type": "long"})
    name: str
    nested: Tuple[NestedModel, ...]
    dt: datetime.datetime
    id: uuid.UUID


@dataclass(frozen=True)
class HasList(AvroModel):
    values: List[NestedModel]


@dataclass(frozen=True)
class HasOptionalBytes(AvroModel):
    value: Optional[bytes]


class TestAvroModel:
    @pytest.mark.parametrize(
        "instance",
        (
            RecordModel(
                symbol=Symbol.b,
                height=123_321_098,
                name="name of a record",
                nested=(),
                id=uuid.uuid4(),
                dt=datetime.datetime.now(tz=datetime.timezone.utc).replace(microsecond=0),
            ),
            RecordModel(
                symbol=Symbol.a,
                height=-1,
                name="",
                nested=(
                    NestedModel(bool_field=True, values=(1, 2, 3)),
                    NestedModel(bool_field=False, values=()),
                    NestedModel(bool_field=False, values=(3, 2, 1)),
                ),
                id=uuid.UUID(int=0),
                dt=datetime.datetime.now(tz=datetime.timezone.utc).replace(microsecond=0),
            ),
            HasOptionalBytes(value=b"foo bar"),
            HasOptionalBytes(value=None),
        ),
    )
    def test_can_roundtrip_instance(self, instance: AvroModel) -> None:
        with io.BytesIO() as buffer:
            instance.serialize(buffer)
            buffer.seek(0)
            parsed = type(instance).parse(buffer)

        assert parsed == instance
