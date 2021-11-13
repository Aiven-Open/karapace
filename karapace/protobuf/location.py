# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/Location.kt
from typing import Optional


class Location:
    """ Locates a .proto file, or a self.position within a .proto file, on the file system """

    def __init__(self, base: str, path: str, line: int = -1, column: int = -1):
        """  str - The base directory of this location;
              path - The path to this location relative to [base]
              line - The line number of this location, or -1 for no specific line number
              column - The column on the line of this location, or -1 for no specific column
        """
        self.base = base
        self.path = path
        self.line = line
        self.column = column

    def at(self, line: int, column: int) -> 'Location':
        return Location(self.base, self.path, line, column)

    def without_base(self) -> 'Location':
        """ Returns a copy of this location with an empty base. """
        return Location("", self.path, self.line, self.column)

    def with_path_only(self) -> 'Location':
        """ Returns a copy of this location including only its path. """
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

    @staticmethod
    def get(*args) -> Optional['Location']:
        result = None
        if len(args) == 1:  # (path)
            path = args[0]
            result = Location.get("", path)
        if len(args) == 2:  # (base,path)
            path: str = args[1]
            base: str = args[0]
            if base.endswith("/"):
                base = base[:-1]
            result = Location(base, path)

        return result
