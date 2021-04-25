from karapace.protobuf.location import Location
from karapace.protobuf.exception import IllegalStateException
from karapace.protobuf.exception import ProtobufParserRuntimeException


def hex_digit(c: str) -> int:
    if ord(c) in range(ord('0'), ord('9')):
        return ord(c) - ord('0')
    if ord(c) in range(ord('a'), ord('f')):
        return ord('a') + 10
    if ord(c) in range(ord('A'), ord('F')):
        return ord(c) - ord('A') + 10
    return -1


def min_of(a:int, b:int)->int :
    return a if a<b else b


class SyntaxReader:
    data: str
    location: Location
    """ Next character to be read """
    pos: int = 0

    """ The number of newline characters  """
    line: int = 0
    """ The index of the most recent newline character. """
    line_start: int = 0

    def __init__(self, data: str, location: Location):
        self.data = data
        self.location = location

    def exhausted(self) -> bool:
        return self.pos == len(self.data)

    """ Reads a non-whitespace character """

    def read_char(self):
        char = self.peek_char()
        self.pos += 1
        return char

    """ Reads a non-whitespace character 'c' """

    def require(self, c: str):
        self.expect(self.read_char() == c, f"expected '{c}'")

    """ 
    Peeks a non-whitespace character and returns it. The only difference between this and
    [read_char] is that this doesn't consume the char.
    """

    def peek_char(self, ch: str = None):
        if ch:
            if self.peek_char() == ch:
                self.pos += 1
                return True
            else:
                return False
        else:
            self.skip_whitespace(True)
            self.expect(self.pos < len(self.data), "unexpected end of file")
            return self.data[self.pos]

    """ Push back the most recently read character. """

    def push_back(self, ch: str):
        if self.data[self.pos - 1] == ch:
            self.pos -= 1

    """ Reads a quoted or unquoted string and returns it. """

    def read_string(self) -> str:
        self.skip_whitespace(True)
        if self.peek_char() in ["\"", "'"]:
            return self.read_quoted_string()

        else:
            return self.read_word()

    def read_numeric_escape_8_3(self) -> int:
        self.pos -= 1
        return self.read_numeric_escape(8, 3)

    def read_quoted_string(self) -> str:
        start_quote = self.read_char()
        if start_quote != '"' and start_quote != '\'':
            raise IllegalStateException(f" quote expected")

        result: list = []

        while self.pos < len(self.data):
            self.pos += 1
            c = self.data[self.pos]
            if c == start_quote:
                """ Adjacent strings are concatenated. 
                Consume new quote and continue reading. """
                if self.peek_char() == '"' or self.peek_char() == "'":
                    start_quote = self.read_char()
                    continue
                return "".join(result)
            if c == "\\":
                self.expect(self.pos < len(self.data), "unexpected end of file")
                self.pos += 1
                c = self.data[self.pos]
                c = {
                    'a': "\u0007",  # Alert.
                    'b': "\b",  # Backspace.
                    'f': "\u000c",  # Form feed.
                    'n': "\n",  # Newline.
                    'r': "\r",  # Carriage return.
                    't': "\t",  # Horizontal tab.
                    'v': "\u000b",  # Vertical tab.
                    'x': self.read_numeric_escape(16, 2),
                    'X': self.read_numeric_escape(16, 2),
                    '0': self.read_numeric_escape_8_3(),
                    '1': self.read_numeric_escape_8_3(),
                    '2': self.read_numeric_escape_8_3(),
                    '3': self.read_numeric_escape_8_3(),
                    '4': self.read_numeric_escape_8_3(),
                    '5': self.read_numeric_escape_8_3(),
                    '6': self.read_numeric_escape_8_3(),
                    '7': self.read_numeric_escape_8_3()
                }.get(c)

            result.append(c)
            if c == "\n":
                self.newline()

        self.unexpected("unterminated string")

    def read_numeric_escape(self, radix: int, length: int) -> int:
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

    """ Reads a (paren-wrapped), [square-wrapped] or naked symbol name. """

    def read_name(self) -> str:
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

    """ Reads a scalar, map, or type name. """

    def read_data_type(self) -> str:
        name = self.read_word()
        return self.read_self.data_type(name)

    """ Reads a scalar, map, or type name with `name` as a prefix word. """

    def read_data_type(self, name: str) -> str:
        if name == "map":

            self.expect(self.read_char() == '<', "expected '<'")
            key_type = self.read_self.data_type()

            self.expect(self.read_char() == ',', "expected ','")
            value_type = self.read_self.data_type()

            self.expect(self.read_char() == '>', "expected '>'")
            return f"map<{key_type}, {value_type}>"
        else:
            return name

    """ Reads a non-empty word and returns it. """

    def read_word(self) -> str:
        self.skip_whitespace(True)
        start = self.pos
        while self.pos < len(self.data):
            c = self.data[self.pos]
            if c in range('a', 'z') or c in range('A', 'Z') or c in range('0', '9') or c in ['_', '-', '.']:
                self.pos += 1
            else:
                break
        self.expect(start < self.pos, "expected a word")
        return self.data[start:self.pos - start].decode()

    """ Reads an integer and returns it. """

    def read_int(self) -> int:
        tag: str = self.read_word()
        try:
            radix = 10
            if tag.startswith("0x") or tag.startswith("0X"):
                tag = tag[len("0x"):]
                radix = 16
            return int(tag, radix)
        except:
            self.unexpected(f"expected an integer but was {tag}")

    """ Like skip_whitespace(), but this returns a string containing all comment text. By convention,
    comments before a declaration document that declaration. """

    def read_documentation(self) -> str:
        result: str = None
        while True:
            self.skip_whitespace(False)
            if self.pos == len(self.data) or self.data[self.pos] != '/':
                if result:
                    return result
                else:
                    ""
            comment = self.read_comment()
            if result:
                result = f"{result}\n{comment}"
            else:
                result = "$result\n$comment"

    """ Reads a comment and returns its body. """

    def read_comment(self) -> str:
        if self.pos == len(self.data) or self.data[self.pos] != '/':
            raise IllegalStateException()

        self.pos += 1
        tval = -1
        if self.pos < len(self.data):
            self.pos += 1
            tval = int(self.data[self.pos])

        if tval == int('*'):
            result: list
            start_of_line = True
            while self.pos + 1 < len(self.data):
                c: str = self.data[self.pos]

                if c == '*' and self.data[self.pos + 1] == '/':
                    self.pos += 2
                    return "".join(result).strip()

                if c == "\n":
                    result.append("\n")
                    self.newline()
                    start_of_line = True

                if not start_of_line:
                    result.append(c)

                if c == "*":
                    if self.data[self.pos + 1] == ' ':
                        self.pos += 1  # Skip a single leading space, if present.
                        start_of_line = False
                if not c.isspace():
                    result.append(c)
                    start_of_line = False
                self.pos += 1
            self.unexpected("unterminated comment")

        if tval == int('/'):
            if self.pos < len(self.data) and self.data[self.pos] == ' ':
                self.pos += 1  # Skip a single leading space, if present.
            start = self.pos
            while self.pos < len(self.data):
                self.pos += 1
                c = self.data[self.pos]
                if c == "\n":
                    self.newline()
                    break
            return self.data[start:self.pos - 1 - start].decode()
        self.unexpected("unexpected '/'")

    def try_append_trailing_documentation(self, documentation: str) -> str:
        """ Search for a '/' character ignoring spaces and tabs."""
        while self.pos < len(self.data):
            if self.data[self.pos] in [' ', "\t"]:
                self.pos += 1

            if self.data[self.pos] == '/':
                self.pos += 1
                break
            """ Not a whitespace or comment-starting character. Return original documentation. """
            return documentation
        bval = (self.pos < len(self.data) and (self.data[self.pos] == '/' or self.data[self.pos] == '*'))
        # Backtrack to start of comment.
        if not bval: self.pos -= 1
        self.expect(bval, "expected '//' or '/*'")
        is_star = self.data[self.pos] == '*'

        self.pos += 1

        # Skip a single leading space, if present.
        if self.pos < len(self.data) and self.data[self.pos] == ' ':
            self.pos += 1

        start = self.pos
        end: int

        if is_star:
            """ Consume star comment until it closes on the same line."""
            while True:
                self.expect(self.pos < len(self.data), "trailing comment must be closed")
                if self.data[self.pos] == '*' and self.pos + 1 < len(self.data) and self.data[self.pos + 1] == '/':
                    end = self.pos - 1  # The character before '*'.
                    self.pos += 2  # Skip to the character after '/'.
                    break
                self.pos += 1

            """ Ensure nothing follows a trailing star comment."""
            while self.pos < len(self.data):
                self.pos += 1
                c = self.data[self.pos]
                if c == "\n":
                    self.newline()
                    break

                self.expect(c == " " or c == "\t", "no syntax may follow trailing comment")

        else:
            """ Consume comment until newline. """
            while True:
                if self.pos == len(self.data):
                    end = self.pos - 1
                    break
                self.pos += 1
                c = self.data[self.pos]
                if c == "\n":
                    self.newline()
                    end = self.pos - 2  # Account for stepping past the newline.
                    break

        """  Remove trailing whitespace."""
        while end > start and (self.data[end] == " " or self.data[end] == "\t"):
            end -= 1

        if end == start:
            return documentation

        trailing_documentation = self.data[start:end - start + 1]
        if not documentation.strip:
            return trailing_documentation
        return f"{documentation}\n{trailing_documentation}"

    """
    Skips whitespace characters and optionally comments. When this returns, either
    self.pos == self.data.length or a non-whitespace character.
    """

    def skip_whitespace(self, skip_comments: bool):
        while self.pos < len(self.data):
            c = self.data[self.pos]
            if c == " " or c == "\t" or c == "\r" or c == "\n":
                self.pos += 1
                if c == "\n":
                    self.newline()
            if skip_comments and c == "/":
                self.read_comment()

            return

    """ Call this every time a '\n' is encountered. """

    def newline(self):
        self.line += 1
        self.line_start = self.pos

    def location(self) -> Location:
        return self.location.at(self.line + 1, self.pos - self.line_start + 1)

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
        raise ProtobufParserRuntimeException(f"Syntax error in {location.to_string()}: {message}")
