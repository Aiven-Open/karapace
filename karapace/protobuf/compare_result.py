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
    FIELD_CONVERTED_TO_ONE_OF = auto()

    # protobuf compatibility issues is described in at
    # https://yokota.blog/2021/08/26/understanding-protobuf-compatibility/
    def is_compatible(self) -> bool:
        return self not in [
            self.FIELD_LABEL_ALTER, self.FIELD_KIND_ALTER, self.ONE_OF_FIELD_ADD, self.ONE_OF_FIELD_DROP,
            self.FIELD_CONVERTED_TO_ONE_OF
        ]


class ModificationRecord:
    def __init__(self, modification: Modification, path: str):
        self.modification: Modification = modification
        self.path: str = path
        if modification.is_compatible():
            self.message: str = f"Compatible modification {self.modification} found"
        else:
            self.message: str = f"Incompatible modification {self.modification} found"

    def to_str(self):
        return self.message


class CompareResult:
    def __init__(self):
        self.result: list = []
        self.path: list = []
        self.canonical_name: list = []

    def push_path(self, string: str, canonical: bool = False):
        if canonical:
            self.canonical_name.append(str(string))
        self.path.append(str(string))

    def pop_path(self, canonical: bool = False):
        if canonical:
            self.canonical_name.pop()
        self.path.pop()

    def add_modification(self, modification: Modification):
        record = ModificationRecord(modification, ".".join(self.path))
        self.result.append(record)

    def is_compatible(self):
        record: ModificationRecord
        for record in self.result:
            if not record.modification.is_compatible():
                return False
        return True
