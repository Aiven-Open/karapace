from enum import auto, Enum
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.proto_type import ProtoType


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
    TYPE_ALTER = auto()
    FIELD_ADD = auto()
    FIELD_DROP = auto()
    FIELD_MOVE = auto()
    FIELD_LABEL_ALTER = auto()
    FIELD_KIND_ALTER = auto()
    ONE_OF_ADD = auto()
    ONE_OF_DROP = auto()
    ONE_OF_MOVE = auto()
    ONE_OF_FIELD_ADD = auto()
    ONE_OF_FIELD_DROP = auto()
    ONE_OF_FIELD_MOVE = auto()

    # protobuf compatibility issues is described in at
    # https://yokota.blog/2021/08/26/understanding-protobuf-compatibility/
    @classmethod
    def get_incompatible(cls):
        return [cls.FIELD_LABEL_ALTER, cls.FIELD_KIND_ALTER, cls.ONE_OF_FIELD_ADD, cls.ONE_OF_FIELD_DROP]


class ModificationRecord:
    def __init__(self, modification: Modification, path: str):
        self.modification: Modification = modification
        self.path: str = path

    def to_str(self):
        # TODO
        pass


class CompareResult:
    def __init__(self):
        self.result: list = []
        self.path: list = []

    def push_path(self, string: str):
        self.path.append(string)

    def pop_path(self):
        self.path.pop()

    def add_modification(self, modification: Modification):
        record = ModificationRecord(modification, ".".join(self.path))
        self.result.append(record)


class CompareTypes:
    def __init__(self):
        self.self_package_name = ''
        self.other_package_name = ''
        self.self_canonical_name: list = []
        self.other_canonical_name: list = []
        self.self_types = dict()
        self.other_types = dict()
        self.locked_messages = []

    def add_other_type(self, name: str, type_: ProtoType):
        self.other_types[name] = type_

    def add_self_type(self, name: str, type_: ProtoType):
        self.self_types[name] = type_

    def self_type_name(self, type_: ProtoType):
        string: str = type_.string
        name: str
        canonical_name: list = list(self.self_canonical_name)
        if string[0] == '.':
            name = string[1:]
            return self.self_types.get(name)
        else:
            if self.self_package_name != '':
                canonical_name.insert(0, self.self_package_name)
            while canonical_name is not None:
                pretender: str = ".".join(canonical_name) + '.' + string
                t = self.self_types.get(pretender)
                if t is not None:
                    return pretender
            if self.self_types.get(string) is not None:
                return string
        return None

    def lock_message(self, message: MessageElement) -> bool:
        if message in self.locked_messages:
            return False
        self.locked_messages.append(message)
        return True

    def unlock_message(self, message: MessageElement) -> bool:
        if message in self.locked_messages:
            self.locked_messages.remove(message)
            return True
        return False

    def other_type_name(self, type_: ProtoType):
        string: str = type_.string
        name: str
        canonical_name: list = list(self.other_canonical_name)
        if string[0] == '.':
            name = string[1:]
            return self.other_types.get(name)
        else:
            if self.other_package_name != '':
                canonical_name.insert(0, self.other_package_name)
            while canonical_name is not None:
                pretender: str = ".".join(canonical_name) + '.' + string
                t = self.other_types.get(pretender)
                if t is not None:
                    return pretender
            if self.other_types.get(string) is not None:
                return string
        return None
