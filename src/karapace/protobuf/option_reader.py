"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/OptionReader.kt
from dataclasses import dataclass
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.syntax_reader import SyntaxReader


@dataclass
class KindAndValue:
    kind: OptionElement.Kind
    value: object


class OptionReader:
    reader: SyntaxReader

    def __init__(self, reader: SyntaxReader) -> None:
        self.reader = reader

    def read_options(self) -> list:
        """Reads options enclosed in '[' and ']' if they are present and returns them. Returns an empty
        list if no options are present.
        """
        if not self.reader.peek_char("["):
            return []
        result = []
        while True:
            result.append(self.read_option("="))

            # Check for closing ']'
            if self.reader.peek_char("]"):
                break

            # Discard optional ','.
            self.reader.expect(self.reader.peek_char(","), "Expected ',' or ']")
        return result

    def read_option(self, key_value_separator: str) -> OptionElement:
        """Reads a option containing a name, an '=' or ':', and a value."""
        is_extension = self.reader.peek_char() == "["
        is_parenthesized = self.reader.peek_char() == "("
        name = self.reader.read_name()  # Option name.
        if is_extension:
            name = f"[{name}]"

        sub_names = []
        c = self.reader.read_char()
        if c == ".":
            # Read nested field name. For example "baz" in "(foo.bar).baz = 12".
            sub_names = self.reader.read_name().split(".")
            c = self.reader.read_char()

        if key_value_separator == ":" and c == "{":
            # In text format, values which are maps can omit a separator. Backtrack so it can be re-read.
            self.reader.push_back("{")
        else:
            self.reader.expect(c == key_value_separator, f"expected '{key_value_separator}' in option")

        kind_and_value = self.read_kind_and_value()
        kind = kind_and_value.kind
        value = kind_and_value.value
        sub_names.reverse()
        for sub_name in sub_names:
            value = OptionElement(sub_name, kind, value, False)
            kind = OptionElement.Kind.OPTION
        return OptionElement(name, kind, value, is_parenthesized)

    def read_kind_and_value(self) -> KindAndValue:
        """Reads a value that can be a map, list, string, number, boolean or enum."""
        peeked = self.reader.peek_char()
        result: KindAndValue
        if peeked == "{":
            result = KindAndValue(OptionElement.Kind.MAP, self.read_map("{", "}", ":"))
        elif peeked == "[":
            result = KindAndValue(OptionElement.Kind.LIST, self.read_list())
        elif peeked in ('"', "'"):
            result = KindAndValue(OptionElement.Kind.STRING, self.reader.read_string())
        elif ord(str(peeked)) in range(ord("0"), ord("9")) or peeked == "-":
            result = KindAndValue(OptionElement.Kind.NUMBER, self.reader.read_word())
        else:
            word = self.reader.read_word()
            if word == "true":
                result = KindAndValue(OptionElement.Kind.BOOLEAN, "true")
            elif word == "false":
                result = KindAndValue(OptionElement.Kind.BOOLEAN, "false")
            else:
                result = KindAndValue(OptionElement.Kind.ENUM, word)
        return result

    def read_map(self, open_brace: str, close_brace: str, key_value_separator: str) -> dict:
        """Returns a map of string keys and values. This is similar to a JSON object, with ':' and '}'
        surrounding the map, ':' separating keys from values, and ',' or ';' separating entries.
        """
        if self.reader.read_char() != open_brace:
            raise AssertionError()
        result = {}
        while True:
            if self.reader.peek_char(close_brace):
                # If we see the close brace, finish immediately. This handles :}/[] and ,}/,] cases.
                return result

            option = self.read_option(key_value_separator)
            name = option.name
            value = option.value
            if isinstance(value, OptionElement):
                nested = result.get(name)
                if not nested:
                    nested = {}
                    result[name] = nested
                nested[value.name] = value.value
            else:
                # Add the value(s) to any previous values with the same key
                previous = result.get(name)
                if not previous:
                    result[name] = value
                elif isinstance(previous, list):  # Add to previous List
                    self.add_to_list(previous, value)
                else:
                    new_list = []
                    new_list.append(previous)
                    self.add_to_list(new_list, value)
                    result[name] = new_list
            # Discard optional separator.
            if not self.reader.peek_char(","):
                self.reader.peek_char(";")

    @staticmethod
    def add_to_list(_list: list, value: list | str) -> None:
        """Adds an object or objects to a List."""
        if isinstance(value, list):
            for v in list(value):
                _list.append(v)
        else:
            _list.append(value)

    def read_list(self) -> list:
        """Returns a list of values. This is similar to JSON with '[' and ']' surrounding the list and ','
        separating values.
        """
        self.reader.require("[")
        result = []
        while True:
            # If we see the close brace, finish immediately. This handles [] and ,] cases.
            if self.reader.peek_char("]"):
                return result

            result.append(self.read_kind_and_value().value)

            if self.reader.peek_char(","):
                continue
            self.reader.expect(self.reader.peek_char() == "]", "expected ',' or ']'")
