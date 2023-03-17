"""
karapace - main

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from functools import partial
from http import HTTPStatus
from karapace.config import Config
from karapace.rapu import HTTPRequest, HTTPResponse, RestApp
from karapace.utils import json_encode
from typing import Callable, NoReturn, Optional, Union

import aiohttp.web
import time


class KarapaceBase(RestApp):
    def __init__(self, config: Config, not_ready_handler: Optional[Callable[[HTTPRequest], None]] = None) -> None:
        super().__init__(app_name="karapace", config=config, not_ready_handler=not_ready_handler)

        self._process_start_time = time.monotonic()
        self.health_hooks = []
        # Do not use rapu's etag, readiness and other wrapping
        self.app.router.add_route("GET", "/_health", self.health)

        self.kafka_timeout = 10
        self.route("/", callback=self.root_get, method="GET")
        self.log.info("Karapace initialized")

    @staticmethod
    def r(body: Union[dict, list], content_type: str, status: HTTPStatus = HTTPStatus.OK) -> NoReturn:
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

    async def root_get(self) -> NoReturn:
        self.r({}, "application/json")

    async def health(self, _request) -> aiohttp.web.Response:
        resp = {"process_uptime_sec": int(time.monotonic() - self._process_start_time)}
        for hook in self.health_hooks:
            resp.update(await hook())
        return aiohttp.web.Response(
            body=json_encode(resp, binary=True, compact=True),
            status=HTTPStatus.OK.value,
            headers={"Content-Type": "application/json"},
        )


empty_response = partial(KarapaceBase.r, body={}, status=HTTPStatus.NO_CONTENT, content_type="application/json")
