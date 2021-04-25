from karapace.protobuf.syntax_reader import SyntaxReader
from karapace.protobuf.option_element import OptionElement


class KindAndValue:
    kind: OptionElement.Kind
    value: object

    def __init__(self, kind: OptionElement.Kind, value: object):
        self.kind = kind
        self.valuer = value


class OptionReader:
    reader: SyntaxReader

    def __init__(self, reader: SyntaxReader):
        self.reader = reader

    """
    Reads options enclosed in '[' and ']' if they are present and returns them. Returns an empty
    list if no options are present.
    """

    def read_options(self) -> list:
        if not self.reader.peek_char('['):
            return list()
        result: list = list()
        while True:
            result.append(self.read_option('='))

            # Check for closing ']'
            if self.reader.peek_char(']'):
                break

            # Discard optional ','.
            self.reader.expect(self.reader.peek_char(','), "Expected ',' or ']")
        return result

    """ Reads a option containing a name, an '=' or ':', and a value.  """

    def read_option(self, key_value_separator: str) -> OptionElement:
        is_extension = (self.reader.peek_char() == '[')
        is_parenthesized = (self.reader.peek_char() == '(')
        name = self.reader.read_name()  # Option name.
        if is_extension:
            name = f"[{name}]"

        sub_names: list = list()
        c = self.reader.read_char()
        if c == '.':
            # Read nested field name. For example "baz" in "(foo.bar).baz = 12".
            sub_names = self.reader.read_name().split(".")
            c = self.reader.read_char()

        if key_value_separator == ':' and c == '{':
            # In text format, values which are maps can omit a separator. Backtrack so it can be re-read.
            self.reader.push_back('{')
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

    """ Reads a value that can be a map, list, string, number, boolean or enum.  """

    def read_kind_and_value(self) -> KindAndValue:
        peeked = self.reader.peek_char()
        if peeked == '{':
            return KindAndValue(OptionElement.Kind.MAP, self.read_map('{', '}', ':'))
        if peeked == '[':
            return KindAndValue(OptionElement.Kind.LIST, self.read_list())
        if peeked == '"' or peeked == "'":
            return KindAndValue(OptionElement.Kind.STRING, self.reader.read_string())

        if peeked.is_digit() or peeked == '-':
            return KindAndValue(OptionElement.Kind.NUMBER, self.reader.read_word())

        word = self.reader.read_word()
        if word == "true":
            return KindAndValue(OptionElement.Kind.BOOLEAN, "true")
        if word == "false":
            return KindAndValue(OptionElement.Kind.BOOLEAN, "false")
        return KindAndValue(OptionElement.Kind.ENUM, word)

    """
    Returns a map of string keys and values. This is similar to a JSON object, with ':' and '}'
    surrounding the map, ':' separating keys from values, and ',' or ';' separating entries.
    """

    def read_map(self, open_brace: str, close_brace: str, key_value_separator: str) -> dict:
        if self.reader.read_char() != open_brace:
            raise AssertionError()
        result: dict = dict()
        while True:
            if self.reader.peek_char(close_brace):
                # If we see the close brace, finish immediately. This handles :}/[] and ,}/,] cases.
                return result

            option = self.read_option(key_value_separator)
            name = option.name
            value = option.value
            if value is OptionElement:
                nested = result[name]
                if not nested:
                    nested = dict()
                    result[name] = nested
                nested[value.name] = value.value
            else:
                # Add the value(s) to any previous values with the same key
                previous = result[name]
                if not previous:
                    result[name] = value
                elif type(previous) is list:  # Add to previous List
                    self.add_to_list(previous, value)
                else:
                    new_list: list = list()
                    new_list.append(previous)
                    self.add_to_list(new_list, value)
                    result[name] = new_list
            # Discard optional separator.
            self.reader.peek_char(',') or self.reader.peek_char(';')

    """ Adds an object or objects to a List.  """

    def add_to_list(self, _list: list, value: object):
        if type(value) is list:
            for v in list(value):
                _list.append(v)
        else:
            _list.append(value)

    """
    * Returns a list of values. This is similar to JSON with '[' and ']' surrounding the list and ','
    * separating values.
    """

    def read_list(self) -> list:
        self.reader.require('[')
        result: list = list()
        while True:
            # If we see the close brace, finish immediately. This handles [] and ,] cases.
            if self.reader.peek_char(']'):
                return result

            result.append(self.read_kind_and_value().value)

            if self.reader.peek_char(','):
                continue
            self.reader.expect(self.reader.peek_char() == ']', "expected ',' or ']'")
