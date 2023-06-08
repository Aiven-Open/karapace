from .duration import Duration
from typing import Literal, overload

import datetime

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
