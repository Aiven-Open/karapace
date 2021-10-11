from karapace.protobuf.exception import IllegalArgumentException
from karapace.protobuf.proto_type import ProtoType
from karapace.protobuf.type_element import TypeElement
from typing import Optional


class CompareTypes:
    def __init__(self):

        self.self_package_name = ''
        self.other_package_name = ''
        self.self_canonical_name: list = []
        self.other_canonical_name: list = []
        self.self_types = dict()
        self.other_types = dict()
        self.locked_messages = []
        self.environment = []

    def add_a_type(self, prefix: str, package_name: str, type_element: TypeElement, types: dict):
        name: str
        if prefix:
            name = prefix + '.' + type_element.name
        else:
            name = type_element.name

        from karapace.protobuf.message_element import MessageElement
        if isinstance(type_element, MessageElement):  # add support of MapEntry messages
            if 'map_entry' in type_element.options:
                from karapace.protobuf.field_element import FieldElement
                key: Optional[FieldElement] = None
                value: Optional[FieldElement] = None
                for f in type_element.fields:
                    if f.name == 'key':
                        key = f
                        break
                for f in type_element.fields:
                    if f.name == 'value':
                        value = f
                        break
                types[name] = TypeRecordMap(package_name, type_element, key, value)
            else:
                types[name] = TypeRecord(package_name, type_element)
        else:
            types[name] = TypeRecord(package_name, type_element)

        for t in type_element.nested_types:
            self.add_a_type(name, package_name, t, types)

    def add_self_type(self, package_name: str, type_element: TypeElement):
        self.add_a_type(package_name, package_name, type_element, self.self_types)

    def add_other_type(self, package_name: str, type_element: TypeElement):
        self.add_a_type(package_name, package_name, type_element, self.other_types)

    def get_self_type(self, t: ProtoType) -> Optional['TypeRecord']:
        name = self.self_type_name(t)
        if name is not None:
            type_record = self.self_types.get(name)
            return type_record
        return None

    def get_other_type(self, t: ProtoType) -> Optional['TypeRecord']:
        name = self.other_type_name(t)
        if name is not None:
            type_record = self.other_types.get(name)
            return type_record
        return None

    def self_type_name(self, t: ProtoType):
        string: str = t.string
        name: str
        canonical_name: list = list(self.self_canonical_name)
        if string[0] == '.':
            name = string[1:]
            if self.self_types.get(name):
                return name
            return None
        if self.self_package_name != '':
            canonical_name.insert(0, self.self_package_name)
        while len(canonical_name) > 0:
            pretender: str = ".".join(canonical_name) + '.' + string
            t = self.self_types.get(pretender)
            if t is not None:
                return pretender
        if self.self_types.get(string) is not None:
            return string
        return None

    def other_type_name(self, t: ProtoType):
        string: str = t.string
        name: str
        canonical_name: list = list(self.other_canonical_name)
        if string[0] == '.':
            name = string[1:]
            if self.other_types.get(name):
                return name
            return None
        if self.other_package_name != '':
            canonical_name.insert(0, self.other_package_name)
        while len(canonical_name) > 0:
            pretender: str = ".".join(canonical_name) + '.' + string
            t = self.other_types.get(pretender)
            if t is not None:
                return pretender
        if self.other_types.get(string) is not None:
            return string
        return None

    def self_type_short_name(self, t: ProtoType):
        name = self.self_type_name(t)
        if name is None:
            raise IllegalArgumentException(f"Cannot determine message type {t}")
        type_record: TypeRecord = self.self_types.get(name)
        if name.startswith(type_record.package_name):
            return name[(len(type_record.package_name) + 1):]
        return name

    def other_type_short_name(self, t: ProtoType):
        name = self.other_type_name(t)
        if name is None:
            raise IllegalArgumentException(f"Cannot determine message type {t}")
        type_record: TypeRecord = self.other_types.get(name)
        if name.startswith(type_record.package_name):
            return name[(len(type_record.package_name) + 1):]
        return name

    def lock_message(self, message: object) -> bool:
        if message in self.locked_messages:
            return False
        self.locked_messages.append(message)
        return True

    def unlock_message(self, message: object) -> bool:
        if message in self.locked_messages:
            self.locked_messages.remove(message)
            return True
        return False


class TypeRecord:
    def __init__(self, package_name: str, type_element: TypeElement):
        self.package_name = package_name
        self.type_element = type_element


class TypeRecordMap(TypeRecord):
    def __init__(self, package_name: str, type_element: TypeElement, key, value):
        super().__init__(package_name, type_element)
        try:
            from karapace.protobuf.field_element import FieldElement
            self.key: FieldElement = key
            self.value: FieldElement = value
        except Exception:
            raise IllegalArgumentException("TypeRecordMap")

    def map_type(self) -> ProtoType:
        return ProtoType.get2(f"map<{self.key.element_type}, {self.value.element_type}>")
