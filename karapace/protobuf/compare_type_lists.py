from itertools import chain
from karapace.protobuf.compare_result import CompareResult, Modification
from karapace.protobuf.compare_type_storage import CompareTypes
from karapace.protobuf.enum_element import EnumElement
from karapace.protobuf.exception import IllegalStateException
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.type_element import TypeElement
from typing import List


def compare_type_lists(
    self_types_list: List[TypeElement],
    other_types_list: List[TypeElement],
    result: CompareResult,
    compare_types: CompareTypes,
) -> CompareResult:
    self_types = {}
    other_types = {}
    self_indexes = {}
    other_indexes = {}

    type_: TypeElement
    for i, type_ in enumerate(self_types_list):
        self_types[type_.name] = type_
        self_indexes[type_.name] = i
        compare_types.add_self_type(compare_types.self_package_name, type_)

    for i, type_ in enumerate(other_types_list):
        other_types[type_.name] = type_
        other_indexes[type_.name] = i
        compare_types.add_other_type(compare_types.other_package_name, type_)

    for name in chain(self_types.keys(), other_types.keys() - self_types.keys()):

        result.push_path(str(name), True)

        if self_types.get(name) is None and other_types.get(name) is not None:
            if isinstance(other_types[name], MessageElement):
                result.add_modification(Modification.MESSAGE_ADD)
            elif isinstance(other_types[name], EnumElement):
                result.add_modification(Modification.ENUM_ADD)
            else:
                raise IllegalStateException("Instance of element is not applicable")
        elif self_types.get(name) is not None and other_types.get(name) is None:
            if isinstance(self_types[name], MessageElement):
                result.add_modification(Modification.MESSAGE_DROP)
            elif isinstance(self_types[name], EnumElement):
                result.add_modification(Modification.ENUM_DROP)
            else:
                raise IllegalStateException("Instance of element is not applicable")
        else:
            if other_indexes[name] != self_indexes[name]:
                if isinstance(self_types[name], MessageElement):
                    # incompatible type
                    result.add_modification(Modification.MESSAGE_MOVE)
                else:
                    raise IllegalStateException("Instance of element is not applicable")
            else:
                if isinstance(self_types[name], MessageElement) and isinstance(other_types[name], MessageElement):
                    self_types[name].compare(other_types[name], result, compare_types)
                elif isinstance(self_types[name], EnumElement) and isinstance(other_types[name], EnumElement):
                    self_types[name].compare(other_types[name], result, compare_types)
                else:
                    # incompatible type
                    result.add_modification(Modification.TYPE_ALTER)
        result.pop_path(True)

    return result
