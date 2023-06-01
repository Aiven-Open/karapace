"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/Location.kt
from __future__ import annotations

from dataclasses import replace
from karapace.dataclasses import default_dataclass
from typing import overload


@default_dataclass
class Location:
    """Locates a .proto file, or a self.position within a .proto file, on the file system"""

    base: str
    """The base directory of this location."""
    path: str
    """The path to this location relative to [base]."""
    line: int = -1
    """ The line number of this location, or -1 for no specific line number."""
    column: int = -1
    """The column on the line of this location, or -1 for no specific column."""

    def at(self, line: int, column: int) -> Location:
        return replace(self, line=line, column=column)

    def without_base(self) -> Location:
        """Returns a copy of this location with an empty base."""
        return replace(self, base="")

    def with_path_only(self) -> Location:
        """Returns a copy of this location including only its path."""
        return replace(self, base="", line=-1, column=-1)

    def __str__(self) -> str:
        result = ""
        if self.base:
            result += self.base + "/"

        result += self.path

        if self.line != -1:
            result += ":"
            result += str(self.line)
            if self.column != -1:
                result += ":"
                result += str(self.column)

        return result

    @staticmethod
    @overload
    def get(path: str, /) -> Location:
        ...

    @staticmethod
    @overload
    def get(base: str, path: str, /) -> Location:
        ...

    @staticmethod
    def get(*args: str) -> Location:
        if len(args) == 1:  # (path)
            path = args[0]
            return Location.get("", path)
        if len(args) == 2:  # (base,path)
            path = args[1]
            base = args[0]
            if base.endswith("/"):
                base = base[:-1]
            return Location(base=base, path=path)

        raise NotImplementedError
