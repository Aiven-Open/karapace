def check(q: bool, message: str):
    if not q:
        raise IllegalStateException(message)


def require(q: bool, message: str):
    if not q:
        raise IllegalArgumentException(message)


def options_to_list(a: list) -> list:
    # TODO
    return a


class IllegalStateException(Exception):
    def __init__(self, message="IllegalStateException"):
        self.message = message
        super().__init__(self.message)


class IntRange(list):
    pass


class IllegalArgumentException(Exception):
    def __init__(self, message="IllegalArgumentException"):
        self.message = message
        super().__init__(self.message)


class String(str):
    pass


class Any(object):
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
    min: object
    max: object

    def __init__(self, min, max):
        self.min = min
        self.max = max

    def __str__(self) -> str:
        return f"{self.min}..{self.max}"
