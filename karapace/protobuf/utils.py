# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/Util.kt
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from karapace.protobuf.option_element import OptionElement


def append_documentation(data: List[str], documentation: str) -> None:
    if not documentation:
        return

    lines: list = documentation.split("\n")

    if len(lines) > 1 and not lines[-1]:
        lines.pop()

    for line in lines:
        data.append("// ")
        data.append(line)
        data.append("\n")


def append_options(data: List[str], options: List['OptionElement']) -> None:
    count = len(options)
    if count == 1:
        data.append('[')
        data.append(try_to_schema(options[0]))
        data.append(']')
        return

    data.append("[\n")
    for i in range(0, count):
        if i < count - 1:
            endl = ","
        else:
            endl = ""
        append_indented(data, try_to_schema(options[i]) + endl)
    data.append(']')


def try_to_schema(obj: 'OptionElement') -> str:
    try:
        return obj.to_schema()
    except AttributeError:
        if isinstance(obj, str):
            return obj
        raise


def append_indented(data: List[str], value: str) -> None:
    lines = value.split("\n")
    if len(lines) > 1 and not lines[-1]:
        del lines[-1]

    for line in lines:
        data.append("  ")
        data.append(line)
        data.append("\n")


MIN_TAG_VALUE = 1
MAX_TAG_VALUE = ((1 << 29) & 0xffffffffffffffff) - 1  # 536,870,911

RESERVED_TAG_VALUE_START = 19000
RESERVED_TAG_VALUE_END = 19999
