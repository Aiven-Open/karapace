from dataclasses import dataclass, field
from enum import auto, Enum


class Modification(Enum):
    # TODO
    PACKAGE_ALTER = auto()
    SYNTAX_ALTER = auto()
    MESSAGE_ADD = auto()
    MESSAGE_DROP = auto()
    MESSAGE_MOVE = auto()
    ENUM_CONSTANT_ADD = auto()
    ENUM_CONSTANT_ALTER = auto()
    ENUM_CONSTANT_DROP = auto()
    ENUM_ADD = auto()
    ENUM_DROP = auto()
    TYPE_ALTER = auto()
    FIELD_ADD = auto()
    FIELD_DROP = auto()
    FIELD_MOVE = auto()
    FIELD_LABEL_ALTER = auto()
    FIELD_NAME_ALTER = auto()
    FIELD_KIND_ALTER = auto()
    FIELD_TYPE_ALTER = auto()
    ONE_OF_ADD = auto()
    ONE_OF_DROP = auto()
    ONE_OF_MOVE = auto()
    ONE_OF_FIELD_ADD = auto()
    ONE_OF_FIELD_DROP = auto()
    ONE_OF_FIELD_MOVE = auto()
    FEW_FIELDS_CONVERTED_TO_ONE_OF = auto()

    # protobuf compatibility issues is described in at
    # https://yokota.blog/2021/08/26/understanding-protobuf-compatibility/
    def is_compatible(self) -> bool:
        return self not in [
            self.MESSAGE_MOVE, self.MESSAGE_DROP, self.FIELD_LABEL_ALTER, self.FIELD_KIND_ALTER, self.FIELD_TYPE_ALTER,
            self.ONE_OF_FIELD_DROP, self.FEW_FIELDS_CONVERTED_TO_ONE_OF
        ]


@dataclass
class ModificationRecord:
    modification: Modification
    path: str
    message: str = field(init=False)

    def __post_init__(self) -> None:
        if self.modification.is_compatible():
            self.message = f"Compatible modification {self.modification} found"
        else:
            self.message = f"Incompatible modification {self.modification} found"

    def to_str(self) -> str:
        return self.message


class CompareResult:
    def __init__(self) -> None:
        self.result = []
        self.path = []
        self.canonical_name = []

    def push_path(self, name_element: str, canonical: bool = False) -> None:
        if canonical:
            self.canonical_name.append(name_element)
        self.path.append(name_element)

    def pop_path(self, canonical: bool = False) -> None:
        if canonical:
            self.canonical_name.pop()
        self.path.pop()

    def add_modification(self, modification: Modification) -> None:
        record = ModificationRecord(modification, ".".join(self.path))
        self.result.append(record)

    def is_compatible(self) -> bool:
        record: ModificationRecord
        for record in self.result:
            if not record.modification.is_compatible():
                return False
        return True
