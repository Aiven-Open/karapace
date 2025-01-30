import datetime
from typing import Literal, overload

from .duration import Duration

@overload
def parse_duration(
    datestring: str,
) -> datetime.timedelta | Duration: ...
@overload
def parse_duration(
    datestring: str,
    as_timedelta_if_possible: Literal[False],
) -> Duration: ...
def duration_isoformat(
    tduration: Duration | datetime.timedelta,
    format: str = ...,
) -> str: ...
