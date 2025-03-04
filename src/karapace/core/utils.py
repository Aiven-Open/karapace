"""
karapace - utils

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from .typing import ArgJsonData, JsonData
from aiohttp.web_log import AccessLogger
from aiohttp.web_request import BaseRequest
from aiohttp.web_response import StreamResponse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import AnyStr, cast, IO, Literal, NoReturn, overload, TypeVar

import importlib
import logging
import signal
import time

if importlib.util.find_spec("ujson"):
    from ujson import JSONDecodeError  # noqa: F401

    import ujson as json
else:
    from json import JSONDecodeError  # noqa: F401

    import json

NS_BLACKOUT_DURATION_SECONDS = 120
LOG = logging.getLogger(__name__)


def _isoformat(datetime_obj: datetime) -> str:
    """Return datetime to ISO 8601 variant suitable for users.

    Assume UTC for datetime objects without a timezone, always use the Z
    timezone designator.
    """
    if datetime_obj.tzinfo:
        datetime_obj = datetime_obj.astimezone(timezone.utc).replace(tzinfo=None)
    return datetime_obj.isoformat() + "Z"


@overload
def default_json_serialization(obj: datetime) -> str: ...


@overload
def default_json_serialization(obj: timedelta) -> float: ...


@overload
def default_json_serialization(obj: Decimal) -> str: ...


@overload
def default_json_serialization(obj: MappingProxyType) -> dict: ...


def default_json_serialization(
    obj: datetime | timedelta | Decimal | MappingProxyType,
) -> str | float | dict:
    if isinstance(obj, datetime):
        return _isoformat(obj)
    if isinstance(obj, timedelta):
        return obj.total_seconds()
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, MappingProxyType):
        return dict(obj)

    assert_never(f"Object of type {obj.__class__.__name__!r} is not JSON serializable")


@overload
def json_encode(
    obj: ArgJsonData,
    *,
    sort_keys: bool | None = ...,
    compact: bool | None = ...,
    indent: int | None = ...,
) -> str: ...


@overload
def json_encode(
    obj: ArgJsonData,
    *,
    binary: Literal[True] = ...,
    sort_keys: bool | None = ...,
    compact: bool | None = ...,
    indent: int | None = ...,
) -> bytes: ...


def json_encode(
    obj: ArgJsonData,
    *,
    binary: bool = False,
    sort_keys: bool | None = None,
    compact: bool | None = None,
    indent: int | None = None,
) -> AnyStr:
    kwargs = {}
    if indent is not None:
        kwargs["indent"] = indent
    if compact is not False and indent is None:
        kwargs["separators"] = (",", ":")
    if sort_keys is True:
        kwargs["sort_keys"] = True
    result = json.dumps(obj, default=default_json_serialization, **kwargs)
    return result.encode("utf8") if binary is True else result


T = TypeVar("T")


@overload
def json_decode(content: AnyStr | IO[AnyStr]) -> JsonData: ...


@overload
def json_decode(content: AnyStr | IO[AnyStr], assume_type: type[T]) -> T: ...


def json_decode(
    content: AnyStr | IO[AnyStr],
    # This argument is only used to pass onto cast() via a type var, it has no runtime
    # usage.
    assume_type: type[T] | None = None,
) -> JsonData | T:
    if isinstance(content, (str, bytes)):
        return cast("T | None", json.loads(content))
    return cast("T | None", json.load(content))


def assert_never(value: NoReturn) -> NoReturn:
    raise RuntimeError(f"This code should never be reached, got: {value}")


def get_project_root() -> Path:
    return Path(__file__).parent.parent


class Timeout(Exception):
    pass


@dataclass(frozen=True)
class Expiration:
    start_time: float
    deadline: float

    @classmethod
    def from_timeout(cls, timeout: float) -> Expiration:
        start_time = time.monotonic()
        deadline = start_time + timeout
        return cls(start_time, deadline)

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.start_time

    def is_expired(self) -> bool:
        return time.monotonic() > self.deadline

    def raise_timeout_if_expired(self, msg_format: str, *args: object, **kwargs: object) -> None:
        """Raise `Timeout` if this object is expired.

        Note:
            This method is supposed to be used in a loop, e.g.:

                expiration = Expiration.from_timeout(timeout=60)
                while is_data_ready(data):
                    expiration.raise_timeout_if_expired("something about", data)
                    data = gather_data()

            The exception message should be meaningful, so it may format data
            into the message itself. However formatting is expensive and should
            be done only when the deadline is expired, so this uses a similar
            interface to `logging.<level>()`.
        """
        if self.is_expired():
            raise Timeout(msg_format.format(*args, **kwargs))


class DebugAccessLogger(AccessLogger):
    """
    Logs access logs as DEBUG instead of INFO.
    Source: https://github.com/aio-libs/aiohttp/blob/d01e257da9b37c35c68b3931026a2d918c271446/aiohttp/web_log.py#L191-L210
    """

    def log(
        self,
        request: BaseRequest,
        response: StreamResponse,
        time: float,
    ) -> None:
        try:
            fmt_info = self._format_line(request, response, time)

            values = list()
            extra = dict()
            for key, value in fmt_info:
                values.append(value)

                if key.__class__ is str:
                    extra[key] = value
                else:
                    k1, k2 = key
                    dct = extra.get(k1, {})
                    dct[k2] = value
                    extra[k1] = dct

            self.logger.debug(self._log_format % tuple(values), extra=extra)
        except Exception:
            self.logger.exception("Error in logging")


def shutdown():
    """
    Send a SIGTERM into the current running application process, which should initiate shutdown logic.
    """
    LOG.warning("=======> Sending shutdown signal `SIGTERM` to Application process <=======")
    signal.raise_signal(signal.SIGTERM)
