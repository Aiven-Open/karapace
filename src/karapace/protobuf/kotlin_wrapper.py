"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from dataclasses import dataclass

import textwrap


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
            new_lines.append(line[idx + 1 :])

    return "\n".join(new_lines)


@dataclass
class KotlinRange:
    minimum: int
    maximum: int

    def __str__(self) -> str:
        return f"{self.minimum}..{self.maximum}"
