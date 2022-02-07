"""
karapace - main

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""

from functools import partial
from http import HTTPStatus
from karapace.rapu import HTTPResponse, RestApp
from typing import NoReturn, Union

import asyncio
import logging
import os


class KarapaceBase(RestApp):
    def __init__(self, config: dict) -> None:
        sentry_config = config.get("sentry", {"dsn": None}).copy()
        super().__init__(app_name="karapace", sentry_config=sentry_config)

        self.kafka_timeout = 10
        self.config = config
        self._sentry_config = sentry_config
        if os.environ.get("SENTRY_DSN"):
            self._sentry_config["dsn"] = os.environ["SENTRY_DSN"]
        if "tags" not in self._sentry_config:
            self._sentry_config["tags"] = {}
        self._sentry_config["tags"]["app"] = "Karapace"

        self.route("/", callback=self.root_get, method="GET")
        self.log = logging.getLogger("Karapace")
        self.app.on_startup.append(self.create_http_client)
        self.master_lock = asyncio.Lock()
        self.log.info("Karapace initialized")
        self.app.on_shutdown.append(self.close_by_app)

    async def close_by_app(self, app):
        # pylint: disable=unused-argument
        await self.close()

    async def close(self) -> None:
        pass

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


empty_response = partial(KarapaceBase.r, body={}, status=HTTPStatus.NO_CONTENT, content_type="application/json")
