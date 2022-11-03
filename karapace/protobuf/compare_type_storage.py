from dataclasses import dataclass
from karapace.protobuf.compare_result import CompareResult
from karapace.protobuf.exception import IllegalArgumentException
from karapace.protobuf.proto_type import ProtoType
from karapace.protobuf.type_element import TypeElement
from typing import Dict, List, Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from karapace.protobuf.field_element import FieldElement
    from karapace.protobuf.message_element import MessageElement


def compute_name(t: ProtoType, result_path: List[str], package_name: str, types: dict) -> Optional[str]:
    string = t.string

    if string.startswith("."):
        name = string[1:]
        if types.get(name):
            return name
        return None
    canonical_name = list(result_path)
    if package_name:
        canonical_name.insert(0, package_name)
    while len(canonical_name) > 0:
        pretender: str = ".".join(canonical_name) + "." + string
        pt = types.get(pretender)
        if pt is not None:
            return pretender
        canonical_name.pop()
    if types.get(string):
        return string
    return None


class CompareTypes:
    def __init__(self, self_package_name: str, other_package_name: str, result: CompareResult) -> None:

        self.self_package_name = self_package_name
        self.other_package_name = other_package_name
        self.self_types: Dict[str, Union[TypeRecord, TypeRecordMap]] = {}
        self.other_types: Dict[str, Union[TypeRecord, TypeRecordMap]] = {}
        self.locked_messages: List["MessageElement"] = []
        self.environment: List["MessageElement"] = []
        self.result = result

    def add_a_type(self, prefix: str, package_name: str, type_element: TypeElement, types: dict) -> None:
        name: str
        if prefix:
            name = prefix + "." + type_element.name
        else:
            name = type_element.name
        from karapace.protobuf.field_element import FieldElement
        from karapace.protobuf.message_element import MessageElement

        if isinstance(type_element, MessageElement):  # add support of MapEntry messages
            if "map_entry" in type_element.options:
                key: Optional[FieldElement] = next((f for f in type_element.fields if f.name == "key"), None)
                value: Optional[FieldElement] = next((f for f in type_element.fields if f.name == "value"), None)
                types[name] = TypeRecordMap(package_name, type_element, key, value)
            else:
                types[name] = TypeRecord(package_name, type_element)
        else:
            types[name] = TypeRecord(package_name, type_element)

        for t in type_element.nested_types:
            self.add_a_type(name, package_name, t, types)

    def add_self_type(self, package_name: str, type_element: TypeElement) -> None:
        self.add_a_type(package_name, package_name, type_element, self.self_types)

    def add_other_type(self, package_name: str, type_element: TypeElement) -> None:
        self.add_a_type(package_name, package_name, type_element, self.other_types)

    def get_self_type(self, t: ProtoType) -> Union[None, "TypeRecord", "TypeRecordMap"]:
        name = compute_name(t, self.result.path, self.self_package_name, self.self_types)
        if name is not None:
            type_record = self.self_types.get(name)
            return type_record
        return None

    def get_other_type(self, t: ProtoType) -> Union[None, "TypeRecord", "TypeRecordMap"]:
        name = compute_name(t, self.result.path, self.other_package_name, self.other_types)
        if name is not None:
            type_record = self.other_types.get(name)
            return type_record
        return None

    def self_type_short_name(self, t: ProtoType) -> Optional[str]:
        name = compute_name(t, self.result.path, self.self_package_name, self.self_types)
        if name is None:
            raise IllegalArgumentException(f"Cannot determine message type {t}")
        type_record: TypeRecord = self.self_types.get(name)
        package_name = type_record.package_name
        if package_name is None:
            package_name = ""
        if name.startswith(package_name):
            return name[(len(package_name) + 1) :]

        return name

    def other_type_short_name(self, t: ProtoType) -> Optional[str]:
        name = compute_name(t, self.result.path, self.other_package_name, self.other_types)
        if name is None:
            raise IllegalArgumentException(f"Cannot determine message type {t}")
        type_record: TypeRecord = self.other_types.get(name)
        package_name = type_record.package_name
        if package_name is None:
            package_name = ""
        if name.startswith(package_name):
            return name[(len(package_name) + 1) :]
        return name

    def lock_message(self, message: "MessageElement") -> bool:
        if message in self.locked_messages:
            return False
        self.locked_messages.append(message)
        return True

    def unlock_message(self, message: "MessageElement") -> bool:
        if message in self.locked_messages:
            self.locked_messages.remove(message)
            return True
        return False


@dataclass
class TypeRecord:
    package_name: str
    type_element: TypeElement


class TypeRecordMap(TypeRecord):
    def __init__(
        self, package_name: str, type_element: TypeElement, key: Optional["FieldElement"], value: Optional["FieldElement"]
    ) -> None:
        super().__init__(package_name, type_element)
        self.key = key
        self.value = value

    def map_type(self) -> ProtoType:
        return ProtoType.get2(f"map<{self.key.element_type}, {self.value.element_type}>")
