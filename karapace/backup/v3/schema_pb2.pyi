from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ChecksumAlgorithm(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    CHECKSUM_ALGORITHM_UNSPECIFIED: _ClassVar[ChecksumAlgorithm]
    CHECKSUM_ALGORITHM_XXHASH3_64_BE: _ClassVar[ChecksumAlgorithm]
CHECKSUM_ALGORITHM_UNSPECIFIED: ChecksumAlgorithm
CHECKSUM_ALGORITHM_XXHASH3_64_BE: ChecksumAlgorithm

class Metadata(_message.Message):
    __slots__ = ["version", "tool_name", "tool_version", "create_time", "estimated_record_count", "topic_name", "partition_count", "checksum_algorithm"]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    TOOL_NAME_FIELD_NUMBER: _ClassVar[int]
    TOOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    CREATE_TIME_FIELD_NUMBER: _ClassVar[int]
    ESTIMATED_RECORD_COUNT_FIELD_NUMBER: _ClassVar[int]
    TOPIC_NAME_FIELD_NUMBER: _ClassVar[int]
    PARTITION_COUNT_FIELD_NUMBER: _ClassVar[int]
    CHECKSUM_ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    version: int
    tool_name: str
    tool_version: str
    create_time: int
    estimated_record_count: int
    topic_name: str
    partition_count: int
    checksum_algorithm: ChecksumAlgorithm
    def __init__(self, version: _Optional[int] = ..., tool_name: _Optional[str] = ..., tool_version: _Optional[str] = ..., create_time: _Optional[int] = ..., estimated_record_count: _Optional[int] = ..., topic_name: _Optional[str] = ..., partition_count: _Optional[int] = ..., checksum_algorithm: _Optional[_Union[ChecksumAlgorithm, str]] = ...) -> None: ...

class Envelope(_message.Message):
    __slots__ = ["record", "checksum"]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    record: Record
    checksum: bytes
    def __init__(self, record: _Optional[_Union[Record, _Mapping]] = ..., checksum: _Optional[bytes] = ...) -> None: ...

class Record(_message.Message):
    __slots__ = ["key", "value", "headers", "partition", "offset", "timestamp_ms"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_MS_FIELD_NUMBER: _ClassVar[int]
    key: bytes
    value: bytes
    headers: _containers.RepeatedCompositeFieldContainer[Header]
    partition: int
    offset: int
    timestamp_ms: int
    def __init__(self, key: _Optional[bytes] = ..., value: _Optional[bytes] = ..., headers: _Optional[_Iterable[_Union[Header, _Mapping]]] = ..., partition: _Optional[int] = ..., offset: _Optional[int] = ..., timestamp_ms: _Optional[int] = ...) -> None: ...

class Header(_message.Message):
    __slots__ = ["key", "value"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: bytes
    def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
