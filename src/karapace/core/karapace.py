"""
karapace - main

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from aiohttp.web_request import Request
from collections.abc import Awaitable, Callable
from functools import partial
from http import HTTPStatus
from karapace.core.config import Config
from karapace.core.dataclasses import default_dataclass
from karapace.core.rapu import HTTPRequest, HTTPResponse, RestApp
from karapace.core.typing import JsonObject
from karapace.core.utils import json_encode
from karapace.core.version import __version__
from typing import NoReturn, TypeAlias

import aiohttp.web
import time


@default_dataclass
class HealthCheck:
    status: JsonObject
    healthy: bool


HealthHook: TypeAlias = Callable[[], Awaitable[HealthCheck]]


class KarapaceBase(RestApp):
    def __init__(self, config: Config, not_ready_handler: Callable[[HTTPRequest], None] | None = None) -> None:
        super().__init__(app_name="karapace", config=config, not_ready_handler=not_ready_handler)

        self._process_start_time = time.monotonic()
        self.health_hooks: list[HealthHook] = []
        # Do not use rapu's etag, readiness and other wrapping
        self.app.router.add_route("GET", "/_health", self.health)

        self.kafka_timeout = 10
        self.route("/", callback=self.root_get, method="GET")
        self.log.info("Karapace initialized")

    @staticmethod
    def r(body: dict | list, content_type: str, status: HTTPStatus = HTTPStatus.OK) -> NoReturn:
        raise HTTPResponse(
            body=body,
            status=status,
            content_type=content_type,
            headers={},
        )

    @staticmethod
    def internal_error(message: str, content_type: str) -> NoReturn:
        KarapaceBase.r(
            content_type=content_type,
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
            body={"message": message, "error_code": HTTPStatus.INTERNAL_SERVER_ERROR.value},
        )

    @staticmethod
    def unprocessable_entity(message: str, sub_code: int, content_type: str) -> NoReturn:
        KarapaceBase.r(
            content_type=content_type,
            status=HTTPStatus.UNPROCESSABLE_ENTITY,
            body={"message": message, "error_code": sub_code},
        )

    @staticmethod
    def topic_entity(message: str, sub_code: int, content_type: str) -> NoReturn:
        KarapaceBase.r(
            content_type=content_type,
            status=HTTPStatus.UNPROCESSABLE_ENTITY,
            body={"message": message, "error_code": sub_code},
        )

    @staticmethod
    def not_found(message: str, sub_code: int, content_type: str) -> NoReturn:
        KarapaceBase.r(
            content_type=content_type, status=HTTPStatus.NOT_FOUND, body={"message": message, "error_code": sub_code}
        )

    @staticmethod
    def service_unavailable(message: str, sub_code: int, content_type: str) -> NoReturn:
        KarapaceBase.r(
            content_type=content_type,
            status=HTTPStatus.SERVICE_UNAVAILABLE,
            body={
                "message": message,
                "error_code": sub_code,
            },
        )

    async def root_get(self) -> NoReturn:
        self.r({}, "application/json")

    async def health(self, _request: Request) -> aiohttp.web.Response:
        resp: JsonObject = {
            "process_uptime_sec": int(time.monotonic() - self._process_start_time),
            "karapace_version": __version__,
        }
        status_code = HTTPStatus.OK
        for hook in self.health_hooks:
            check = await hook()
            resp.update(check.status)
            if not check.healthy:
                status_code = HTTPStatus.SERVICE_UNAVAILABLE
        return aiohttp.web.Response(
            body=json_encode(resp, binary=True, compact=True),
            status=status_code.value,
            headers={"Content-Type": "application/json"},
        )


empty_response = partial(KarapaceBase.r, body={}, status=HTTPStatus.NO_CONTENT, content_type="application/json")
