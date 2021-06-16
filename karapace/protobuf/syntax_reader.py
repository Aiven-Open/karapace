# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/SyntaxReader.kt
from karapace.protobuf.exception import IllegalStateException
from karapace.protobuf.location import Location
from typing import Union


def hex_digit(c: str) -> int:
    if ord(c) in range(ord('0'), ord('9') + 1):
        return ord(c) - ord('0')
    if ord(c) in range(ord('a'), ord('f') + 1):
        return ord(c) - ord('a') + 10
    if ord(c) in range(ord('A'), ord('F') + 1):
        return ord(c) - ord('A') + 10
    return -1


def min_of(a: int, b: int) -> int:
    return a if a < b else b


class SyntaxReader:
    def __init__(self, data: str, location: Location):
        """ Next character to be read """
        self.pos = 0
        """ The number of newline characters  """
        self.line = 0
        """ The index of the most recent newline character. """
        self.line_start = 0
        self.data = data
        self._location = location

    def exhausted(self) -> bool:
        return self.pos == len(self.data)

    def read_char(self):
        """ Reads a non-whitespace character """

        char = self.peek_char()
        self.pos += 1
        return char

    def require(self, c: str):
        """ Reads a non-whitespace character 'c' """
        self.expect(self.read_char() == c, f"expected '{c}'")

    def peek_char(self, ch: str = None):
        """ Peeks a non-whitespace character and returns it. The only difference between this and
        [read_char] is that this doesn't consume the char.
        """

        if ch:
            if self.peek_char() == ch:
                self.pos += 1
                return True
            return False
        self.skip_whitespace(True)
        self.expect(self.pos < len(self.data), "unexpected end of file")
        return self.data[self.pos]

    def push_back(self, ch: str):
        """ Push back the most recently read character. """
        if self.data[self.pos - 1] == ch:
            self.pos -= 1

    def read_string(self) -> str:
        """ Reads a quoted or unquoted string and returns it. """
        self.skip_whitespace(True)
        if self.peek_char() in ['"', "'"]:
            return self.read_quoted_string()
        return self.read_word()

    def read_quoted_string(self) -> str:
        start_quote = self.read_char()
        if start_quote not in ('"', "'"):
            raise IllegalStateException(" quote expected")

        result: list = []

        while self.pos < len(self.data):
            c = self.data[self.pos]
            self.pos += 1
            if c == start_quote:
                if self.peek_char() == '"' or self.peek_char() == "'":
                    # Adjacent strings are concatenated.  Consume new quote and continue reading.
                    start_quote = self.read_char()
                    continue
                return "".join(result)
            if c == "\\":
                self.expect(self.pos < len(self.data), "unexpected end of file")
                c = self.data[self.pos]
                self.pos += 1
                d: Union[str, None] = {
                    'a': "\u0007",  # Alert.
                    'b': "\b",  # Backspace.
                    'f': "\u000c",  # Form feed.
                    'n': "\n",  # Newline.
                    'r': "\r",  # Carriage return.
                    't': "\t",  # Horizontal tab.
                    'v': "\u000b",  # Vertical tab.
                }.get(c)
                if d:
                    c = d
                else:
                    if c in ['x', 'X']:
                        c = self.read_numeric_escape(16, 2)
                    elif ord(c) in range(ord('0'), ord('7') + 1):
                        self.pos -= 1
                        c = self.read_numeric_escape(8, 3)

            result.append(c)
            if c == "\n":
                self.newline()

        self.unexpected("unterminated string")

    def read_numeric_escape(self, radix: int, length: int) -> str:
        value = -1
        end_pos = min_of(self.pos + length, len(self.data))
        while self.pos < end_pos:
            digit = hex_digit(self.data[self.pos])
            if digit == -1 or digit >= radix:
                break

            if value < 0:
                value = digit
            else:
                value = value * radix + digit
            self.pos += 1

        self.expect(value >= 0, "expected a digit after \\x or \\X")
        return chr(value)

    def read_name(self) -> str:
        """ Reads a (paren-wrapped), [square-wrapped] or naked symbol name. """

        c = self.peek_char()
        if c == '(':
            self.pos += 1
            result = self.read_word()
            self.expect(self.read_char() == ')', "expected ')'")
            return result
        if c == '[':
            self.pos += 1
            result = self.read_word()
            self.expect(self.read_char() == ']', "expected ']'")
            return result
        return self.read_word()

    def read_data_type(self) -> str:
        """ Reads a scalar, map, or type name. """

        name = self.read_word()
        return self.read_data_type_by_name(name)

    def read_data_type_by_name(self, name: str) -> str:
        """ Reads a scalar, map, or type name with `name` as a prefix word. """
        if name == "map":
            self.expect(self.read_char() == '<', "expected '<'")
            key_type = self.read_data_type()

            self.expect(self.read_char() == ',', "expected ','")
            value_type = self.read_data_type()

            self.expect(self.read_char() == '>', "expected '>'")
            return f"map<{key_type}, {value_type}>"
        return name

    def read_word(self) -> str:
        """ Reads a non-empty word and returns it. """
        self.skip_whitespace(True)
        start = self.pos
        while self.pos < len(self.data):
            c = self.data[self.pos]
            if ord(c) in range(ord('a'), ord('z') + 1) \
                    or ord(c) in range(ord('A'), ord('Z') + 1) \
                    or ord(c) in range(ord('0'), ord('9') + 1) or c in ['_', '-', '.']:
                self.pos += 1
            else:
                break
        self.expect(start < self.pos, "expected a word")
        return self.data[start:self.pos]

    def read_int(self) -> int:
        """ Reads an integer and returns it. """
        tag: str = self.read_word()
        try:
            radix = 10
            if tag.startswith("0x") or tag.startswith("0X"):
                tag = tag[len("0x"):]
                radix = 16
            return int(tag, radix)
        except OSError as err:
            print("OS error: {0}".format(err))
        except ValueError:
            self.unexpected(f"expected an integer but was {tag}")
        return -22  # this return never be called but mypy think we need it

    def read_documentation(self) -> str:
        """ Like skip_whitespace(), but this returns a string containing all comment text. By convention,
        comments before a declaration document that declaration. """

        result = None
        while True:
            self.skip_whitespace(False)
            if self.pos == len(self.data) or self.data[self.pos] != '/':
                if result:
                    return result
                return ""
            comment = self.read_comment()
            if result:
                result = f"{result}\n{comment}"
            else:
                result = f"{comment}"

    def read_comment(self) -> str:
        """ Reads a comment and returns its body. """
        if self.pos == len(self.data) or self.data[self.pos] != '/':
            raise IllegalStateException()

        self.pos += 1
        tval = -1
        if self.pos < len(self.data):
            tval = ord(self.data[self.pos])
            self.pos += 1
        result: str = ""
        if tval == ord('*'):
            buffer: list = list()
            start_of_line = True
            while self.pos + 1 < len(self.data):
                # pylint: disable=no-else-break
                c: str = self.data[self.pos]
                if c == '*' and self.data[self.pos + 1] == '/':
                    self.pos += 2
                    result = "".join(buffer).strip()
                    break
                elif c == "\n":
                    buffer.append("\n")
                    self.newline()
                    start_of_line = True
                elif not start_of_line:
                    buffer.append(c)
                elif c == "*":
                    if self.data[self.pos + 1] == ' ':
                        self.pos += 1  # Skip a single leading space, if present.
                        start_of_line = False
                elif not c.isspace():
                    buffer.append(c)
                    start_of_line = False
                self.pos += 1
            if not result:
                self.unexpected("unterminated comment")
        elif tval == ord('/'):
            if self.pos < len(self.data) and self.data[self.pos] == ' ':
                self.pos += 1  # Skip a single leading space, if present.
            start = self.pos
            while self.pos < len(self.data):
                c = self.data[self.pos]
                self.pos += 1
                if c == "\n":
                    self.newline()
                    break
            result = self.data[start:self.pos - 1]
        if not result:
            self.unexpected("unexpected '/'")
        return result

    def try_append_trailing_documentation(self, documentation: str) -> str:
        """ Search for a '/' character ignoring spaces and tabs."""
        while self.pos < len(self.data):
            if self.data[self.pos] in [' ', "\t"]:
                self.pos += 1
            elif self.data[self.pos] == '/':
                self.pos += 1
                break
            else:
                # Not a whitespace or comment-starting character. Return original documentation.
                return documentation
        bval = (self.pos < len(self.data) and (self.data[self.pos] == '/' or self.data[self.pos] == '*'))
        if not bval:
            # Backtrack to start of comment.
            self.pos -= 1
        self.expect(bval, "expected '//' or '/*'")
        is_star = self.data[self.pos] == '*'

        self.pos += 1

        # Skip a single leading space, if present.
        if self.pos < len(self.data) and self.data[self.pos] == ' ':
            self.pos += 1

        start = self.pos
        end: int

        if is_star:
            # Consume star comment until it closes on the same line.
            while True:
                self.expect(self.pos < len(self.data), "trailing comment must be closed")
                if self.data[self.pos] == '*' and self.pos + 1 < len(self.data) and self.data[self.pos + 1] == '/':
                    end = self.pos - 1  # The character before '*'.
                    self.pos += 2  # Skip to the character after '/'.
                    break
                self.pos += 1
            # Ensure nothing follows a trailing star comment.
            while self.pos < len(self.data):
                c = self.data[self.pos]
                self.pos += 1
                if c == "\n":
                    self.newline()
                    break

                self.expect(c in [" ", "\t"], "no syntax may follow trailing comment")

        else:
            # Consume comment until newline.
            while True:
                if self.pos == len(self.data):
                    end = self.pos - 1
                    break
                c = self.data[self.pos]
                self.pos += 1
                if c == "\n":
                    self.newline()
                    end = self.pos - 2  # Account for stepping past the newline.
                    break

        # Remove trailing whitespace.
        while end > start and (self.data[end] == " " or self.data[end] == "\t"):
            end -= 1

        if end == start:
            return documentation

        trailing_documentation = self.data[start:end + 1]
        if not documentation.strip():
            return trailing_documentation
        return f"{documentation}\n{trailing_documentation}"

    def skip_whitespace(self, skip_comments: bool):
        """ Skips whitespace characters and optionally comments. When this returns, either
        self.pos == self.data.length or a non-whitespace character.
        """
        while self.pos < len(self.data):
            c = self.data[self.pos]
            if c in [" ", "\t", "\r", "\n"]:
                self.pos += 1
                if c == "\n":
                    self.newline()
            elif skip_comments and c == "/":
                self.read_comment()
            else:
                return

    def newline(self):
        """ Call this every time a '\n' is encountered. """
        self.line += 1
        self.line_start = self.pos

    def location(self) -> Location:
        return self._location.at(self.line + 1, self.pos - self.line_start + 1)

    def expect(self, condition: bool, message: str):
        location = self.location()
        if not condition:
            self.unexpected(message, location)

    def expect_with_location(self, condition: bool, location: Location, message: str):
        if not condition:
            self.unexpected(message, location)

    def unexpected(self, message: str, location: Location = None):
        if not location:
            location = self.location()
        w = f"Syntax error in {str(location)}: {message}"
        raise IllegalStateException(w)
