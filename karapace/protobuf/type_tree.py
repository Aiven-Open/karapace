"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

import copy
from collections.abc import Iterable, Sequence
from karapace.dataclasses import default_dataclass
from typing import Any

import itertools


@default_dataclass
class SourceFileReference:
    reference: str
    import_order: int


@default_dataclass
class TypeTree:
    token: str
    children: list[TypeTree]
    source_reference: SourceFileReference | None
    provider: Any  # todo: replace with the provider of the type!

    def source_reference_tree_recursive(self) -> Iterable[SourceFileReference | None]:
        sources = [] if self.source_reference is None else [self.source_reference]
        for child in self.children:
            sources = itertools.chain(sources, child.source_reference_tree())
        return list(sources)

    def source_reference_tree(self) -> Iterable[SourceFileReference]:
        return filter(lambda x: x is not None, self.source_reference_tree_recursive())

    def inserted_elements(self) -> int:
        """
        Returns the newest element generation accessible from that node.
        Where with the term generation we mean the order for which a message
        has been imported.
        If called on the root of the tree it corresponds with the number of
        fully specified path objects present in the tree.
        """
        return max(reference.import_order for reference in self.source_reference_tree())

    def oldest_matching_import(self) -> int:
        """
        Returns the oldest element generation accessible from that node.
        Where with the term generation we mean the order for which a message
        has been imported.
        """
        return min(reference.import_order for reference in self.source_reference_tree())

    def expand_missing_absolute_path_recursive(self, oldest_import: int) -> Sequence[str]:
        """
        Method that, once called on a node, traverse all the leaf and
        return the oldest imported element with the common postfix.
        This is also the current behaviour
        of protobuf while dealing with a not fully specified path, it seeks for
        the firstly imported message with a matching path.
        """
        if self.source_reference is not None:
            if self.source_reference.import_order == oldest_import:
                return [self.token]
            return []

        for child in self.children:
            maybe_oldest_child = child.expand_missing_absolute_path_recursive(oldest_import)
            if maybe_oldest_child is not None:
                return list(maybe_oldest_child) + [self.token]

        return []

    @staticmethod
    def _type_in_tree(tree: TypeTree, remaining_tokens: list[str]) -> TypeTree | None:
        if remaining_tokens:
            to_seek = remaining_tokens.pop()

            for child in tree.children:
                if child.token == to_seek:
                    return TypeTree._type_in_tree(child, remaining_tokens)
            return None
        return tree

    def type_in_tree(self, remaining_tokens: list[str]) -> TypeTree | None:
        return TypeTree._type_in_tree(self, copy.deepcopy(remaining_tokens))

    def expand_missing_absolute_path(self) -> Sequence[str]:
        oldest_import = self.oldest_matching_import()
        expanded_missing_path = self.expand_missing_absolute_path_recursive(oldest_import)
        assert (
            expanded_missing_path is not None
        ), "each node should have, by construction, at least a leaf that is a fully specified path"
        return expanded_missing_path[:-1]  # skipping myself since I was matched

    @property
    def is_fully_qualified_type(self) -> bool:
        return len(self.children) == 0

    def represent(self, level: int = 0) -> str:
        spacing = " " * 3 * level
        if self.is_fully_qualified_type:
            return f"{spacing}>{self.token}"
        child_repr = "\n".join(child.represent(level=level + 1) for child in self.children)
        return f"{spacing}{self.token} ->\n{child_repr}"

    def __repr__(self) -> str:
        return self.represent()

    # useful to see the definitions of the dataclass once defined:
    # def _pprint(self) -> str:
    #    return (
    #        f"TypeTree("
    #        f"token={self.token},"
    #        f"children={self.children},"
    #        f"source_reference={self.source_reference},"
    #        f"provider={self.provider},"
    #        f")"
    #    )
