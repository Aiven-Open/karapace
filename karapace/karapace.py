"""
karapace - main

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""

from functools import partial
from http import HTTPStatus
from karapace.config import Config
from karapace.rapu import HTTPResponse, RestApp
from typing import NoReturn, Union

import logging


class KarapaceBase(RestApp):
    def __init__(self, config: Config) -> None:
        super().__init__(app_name="karapace", config=config)

        self.kafka_timeout = 10
        self.route("/", callback=self.root_get, method="GET")
        self.log = logging.getLogger("Karapace")
        self.app.on_startup.append(self.create_http_client)
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
