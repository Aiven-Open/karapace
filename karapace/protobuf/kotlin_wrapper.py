from karapace.protobuf.exception import IllegalArgumentException, IllegalStateException


def check(q: bool, message: str):
    if not q:
        raise IllegalStateException(message)


def trim_margin(s: str) -> str:
    lines = s.split("\n")
    new_lines = list()

    for line in lines:
        idx = line.find("|")
        if idx < 0:
            new_lines.append(line)
        else:
            new_lines.append(line[idx + 1:].rstrip())

    if not new_lines[0].strip():
        del new_lines[0]

    if not new_lines[-1].strip():
        del new_lines[-1]

    return "\n".join(new_lines)


def require(q: bool, message: str):
    if not q:
        raise IllegalArgumentException(message)


def options_to_list(a: list) -> list:
    # TODO
    return a


class String(str):
    pass


class Any:
    pass


class StringBuilder(list):
    def append_indented(self: list, value: str):
        lines = value.split("\n")
        if len(lines) > 1 and not lines[-1]:
            lines = lines.pop()

        for line in lines:
            self.append("  ")
            self.append(line)
            self.append("\n")


class OptionsList(list):
    pass


class KotlinRange:
    minimum: int
    maximum: int

    def __init__(self, minimum, maximum):
        self.minimum = minimum
        self.maximum = maximum

    def __str__(self) -> str:
        return f"{self.minimum}..{self.maximum}"
