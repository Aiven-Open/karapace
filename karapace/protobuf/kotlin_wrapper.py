from karapace.protobuf.exception import IllegalArgumentException, IllegalStateException

import textwrap


def check(q: bool, message: str) -> None:
    if not q:
        raise IllegalStateException(message)


def trim_margin(s: str) -> str:
    lines = s.split("\n")
    new_lines = []

    if not textwrap.dedent(lines[0]):
        del lines[0]

    if not textwrap.dedent(lines[-1]):
        del lines[-1]

    for line in lines:
        idx = line.find("|")
        if idx < 0:
            new_lines.append(line)
        else:
            new_lines.append(line[idx + 1:])

    return "\n".join(new_lines)


def require(q: bool, message: str) -> None:
    if not q:
        raise IllegalArgumentException(message)


def options_to_list(a: list) -> list:
    # TODO
    return a


class String(str):
    pass


class Any:
    pass


class OptionsList(list):
    pass


class KotlinRange:
    def __init__(self, minimum, maximum) -> None:
        self.minimum = minimum
        self.maximum = maximum

    def __str__(self) -> str:
        return f"{self.minimum}..{self.maximum}"
