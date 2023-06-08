"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from datetime import timedelta
from functools import cached_property
from isodate import duration_isoformat, parse_duration
from typing import Final

__all__ = ("PollTimeout",)


class PollTimeout:
    """Specifies how long a single poll attempt may take while consuming the topic.

    It may be necessary to adjust this value in case the cluster is slow. The value must be given in ISO8601 duration
    format (e.g. `PT1.5S` for 1,500 milliseconds) and must be at least on second. Defaults to one minute.
    """

    def __init__(self, value: str | timedelta) -> None:
        duration = value if isinstance(value, timedelta) else parse_duration(value)
        # parse_duration() returns Duration objects for values that cannot be
        # represented by a datetime.timedelta.
        if not isinstance(duration, timedelta):
            raise ValueError(f"Poll timeout must be less than one year, got: {duration}")
        if isinstance(duration, timedelta) and duration < timedelta(seconds=1):
            raise ValueError(f"Poll timeout must be at least one second, got: {duration}")
        self.__value: Final = duration

    @classmethod
    def default(cls) -> PollTimeout:
        return cls(timedelta(minutes=1))

    @classmethod
    def of(cls, minutes: int = 0, seconds: int = 0, milliseconds: int = 0) -> PollTimeout:
        """Convenience function to avoid importing ``timedelta``."""
        return PollTimeout(timedelta(minutes=minutes, seconds=seconds, milliseconds=milliseconds))

    def __str__(self) -> str:
        """Returns the ISO8601 formatted value of this poll timeout."""
        return duration_isoformat(self.__value)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(value='{self}')"

    @cached_property
    def milliseconds(self) -> int:
        """Returns this poll timeout in milliseconds, anything smaller than a milliseconds is ignored (no rounding)."""
        return self.__value // timedelta(milliseconds=1)
