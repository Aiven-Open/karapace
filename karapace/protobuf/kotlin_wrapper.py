

def check(q: bool, message: str):
    if not q:
        raise IllegalStateException(message)


def require(q: bool, message: str):
    if not q:
        raise IllegalArgumentException(message)


class IllegalStateException(Exception):
    def __init__(self, message="IllegalStateException"):
        self.message = message
        super().__init__(self.message)


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
