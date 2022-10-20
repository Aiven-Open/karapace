from karapace.dependency import DependencyVerifierResult
from karapace.protobuf.known_dependency import DependenciesHardcoded, KnownDependency
from karapace.protobuf.one_of_element import OneOfElement
from typing import List


class ProtobufDependencyVerifier:
    def __init__(self) -> None:
        self.declared_types: List[str] = []
        self.used_types: List[str] = []
        self.import_path: List[str] = []

    def add_declared_type(self, full_name: str) -> None:
        self.declared_types.append(full_name)

    def add_used_type(self, parent: str, element_type: str) -> None:
        if element_type.find("map<") == 0:
            end = element_type.find(">")
            virgule = element_type.find(",")
            key = element_type[4:virgule]
            value = element_type[virgule + 1 : end]
            value = value.strip()
            self.used_types.append(parent + ";" + key)
            self.used_types.append(parent + ";" + value)
        else:
            self.used_types.append(parent + ";" + element_type)

    def add_import(self, import_name: str) -> None:
        self.import_path.append(import_name)

    def verify(self) -> DependencyVerifierResult:
        declared_index = set(self.declared_types)
        for used_type in self.used_types:
            delimiter = used_type.rfind(";")
            used_type_with_scope = ""
            if delimiter != -1:
                used_type_with_scope = used_type[:delimiter] + "." + used_type[delimiter + 1 :]
                used_type = used_type[delimiter + 1 :]

            if not (
                used_type in DependenciesHardcoded.index
                or KnownDependency.index_simple.get(used_type) is not None
                or KnownDependency.index.get(used_type) is not None
                or used_type in declared_index
                or (delimiter != -1 and used_type_with_scope in declared_index)
                or "." + used_type in declared_index
            ):
                return DependencyVerifierResult(False, f"type {used_type} is not defined")

        return DependencyVerifierResult(True)


def _process_one_of(verifier: ProtobufDependencyVerifier, package_name: str, parent_name: str, one_of: OneOfElement) -> None:
    parent = package_name + "." + parent_name
    for field in one_of.fields:
        verifier.add_used_type(parent, field.element_type)
