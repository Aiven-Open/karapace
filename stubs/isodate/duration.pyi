from decimal import Decimal

import datetime

class Duration:
    months: Decimal
    years: Decimal
    tdelta: datetime.timedelta

    def __init__(
        self,
        days: int = ...,
        seconds: int = ...,
        microseconds: int = ...,
        milliseconds: int = ...,
        minutes: int = ...,
        hours: int = ...,
        weeks: int = ...,
        months: int = ...,
        years: int = ...,
    ) -> None: ...
