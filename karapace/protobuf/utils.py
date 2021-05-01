import builtins


def protobuf_encode(a: str) -> str:
    # TODO: PROTOBUF
    return a


def append_documentation(data: list, documentation: str):
    if not documentation:
        return
    documentation.split()
    lines: list = documentation.split("\n")

    if len(lines) > 1 and lines[-1]:
        lines = lines.pop()

    for line in lines:
        data.append("# ")
        data.append(line)
        data.append("\n")


def append_options(data: list, options: list):
    count = len(options)
    if count == 1:
        data.append('[')
        data.append(options[0].to_schema())
        data.append(']')
        return

    data.append("[\n")
    for i in range(0, count):
        if i < count - 1:
            endl = ","
        else:
            endl = ""
        append_indented(data, options[i].to_schema() + endl)
    data.append(']')


def append_indented(data: list, value: str):
    lines = value.split("\n")
    if len(lines) > 1 and not lines[-1]:
        lines = lines.pop()

    for line in lines:
        data.append("  ")
        data.append(line)
        data.append("\n")


MIN_TAG_VALUE = 1
MAX_TAG_VALUE = ((1 << 29) & 0xffffffffffffffff) - 1  # 536,870,911

RESERVED_TAG_VALUE_START = 19000
RESERVED_TAG_VALUE_END = 19999
""" True if the supplied value is in the valid tag range and not reserved.  """


class MyInt(int):
    def is_valid_tag(self) -> bool:
        return (MIN_TAG_VALUE <= self <= RESERVED_TAG_VALUE_START) or\
               (RESERVED_TAG_VALUE_END + 1 <= self <= MAX_TAG_VALUE + 1)


builtins.int = MyInt
