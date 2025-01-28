"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/Location.kt


class Location:
    """Locates a .proto file, or a self.position within a .proto file, on the file system"""

    def __init__(self, base: str, path: str, line: int = -1, column: int = -1) -> None:
        """str - The base directory of this location;
        path - The path to this location relative to [base]
        line - The line number of this location, or -1 for no specific line number
        column - The column on the line of this location, or -1 for no specific column
        """
        if base.endswith("/"):
            base = base[:-1]
        self.base = base
        self.path = path
        self.line = line
        self.column = column

    def at(self, line: int, column: int) -> "Location":
        return Location(self.base, self.path, line, column)

    def without_base(self) -> "Location":
        """Returns a copy of this location with an empty base."""
        return Location("", self.path, self.line, self.column)

    def with_path_only(self) -> "Location":
        """Returns a copy of this location including only its path."""
        return Location("", self.path, -1, -1)

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


DEFAULT_LOCATION = Location("", "")
