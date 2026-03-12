"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import HTTPException, Request
from karapace.core.utils import json_decode
from karapace.core.config import Config
from karapace.version import __version__
from pydantic import BaseModel
from typing import overload, TypeVar, Union

import aiohttp
import async_timeout
import logging
import os
import ssl

LOG = logging.getLogger(__name__)


BaseModelResponse = TypeVar("BaseModelResponse", bound=BaseModel)
SimpleTypeResponse = TypeVar("SimpleTypeResponse", bound=Union[int, list[int]])


class ForwardClient:
    USER_AGENT = f"Karapace/{__version__}"

    # Only these headers are forwarded to the upstream primary
    _HEADERS_TO_FORWARD: frozenset[str] = frozenset(
        {
            "authorization",
            "content-type",
            "traceparent",
        }
    )

    def __init__(self, config: Config) -> None:
        self.advertised_protocol = config.advertised_protocol
        self._forward_client: aiohttp.ClientSession = aiohttp.ClientSession(headers={"User-Agent": self.USER_AGENT})
        self._ssl_context: ssl.SSLContext | None = None
        if self.advertised_protocol == "https" and config.server_tls_cafile:
            if os.path.exists(config.server_tls_cafile):
                self._ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
                self._ssl_context.load_verify_locations(cafile=config.server_tls_cafile)

    async def close(self) -> None:
        await self._forward_client.close()

    def _acceptable_response_content_type(self, *, content_type: str) -> bool:
        return (
            content_type.startswith("application/") and content_type.endswith("json")
        ) or content_type == "application/octet-stream"

    def _forwardable_headers(self, request: Request) -> dict[str, str]:
        return {k: v for k, v in request.headers.items() if k.lower() in self._HEADERS_TO_FORWARD}

    async def _forward_request_remote(
        self,
        *,
        request: Request,
        primary_url: str,
    ) -> tuple[int, str]:
        LOG.info("Forwarding %s request to remote url: %r since we're not the master", request.method, request.url)
        timeout = 60.0
        func = getattr(self._forward_client, request.method.lower())

        forward_url = f"{primary_url}{request.url.path}"
        if request.url.query:
            forward_url = f"{forward_url}?{request.url.query}"

        headers = self._forwardable_headers(request)

        async with async_timeout.timeout(timeout):
            body_data = await request.body()
            async with func(forward_url, headers=headers, data=body_data, ssl=self._ssl_context) as response:
                response_status = response.status
                content_type = response.headers.get("Content-Type", "")
                response_body = await response.text()
                if not self._acceptable_response_content_type(content_type=content_type):
                    LOG.warning(
                        "Forwarded request got unexpected content type %r (status=%s, url=%s): %s",
                        content_type,
                        response_status,
                        forward_url,
                        response_body,
                    )
                return (response_status, response_body)

    @overload
    async def forward_request_remote(
        self,
        *,
        request: Request,
        primary_url: str,
        response_type: type[BaseModelResponse],
    ) -> BaseModelResponse: ...

    @overload
    async def forward_request_remote(
        self,
        *,
        request: Request,
        primary_url: str,
        response_type: type[SimpleTypeResponse],
    ) -> SimpleTypeResponse: ...

    async def forward_request_remote(
        self,
        *,
        request: Request,
        primary_url: str,
        response_type: type[BaseModelResponse] | type[SimpleTypeResponse],
    ) -> BaseModelResponse | SimpleTypeResponse:
        response_status, body = await self._forward_request_remote(request=request, primary_url=primary_url)

        # Check if the response is an error (4xx or 5xx status codes)
        if response_status >= 400:
            # Try to parse the error response
            try:
                error_data = json_decode(body, assume_type=dict)
                error_code = error_data.get("error_code", response_status)
                error_message = error_data.get("message", "Unknown error")
                raise HTTPException(
                    status_code=response_status,
                    detail={
                        "error_code": error_code,
                        "message": error_message,
                    },
                )
            except (ValueError, TypeError):
                # If we can't parse the error response, raise a generic error
                raise HTTPException(
                    status_code=response_status,
                    detail={
                        "error_code": response_status,
                        "message": body if body else "Unknown error",
                    },
                )

        # Parse successful response
        if response_type is int:
            return int(body)  # type: ignore[return-value]
        if response_type == list[int]:
            return json_decode(body, assume_type=list[int])  # type: ignore[return-value]
        if issubclass(response_type, BaseModel):
            return response_type.parse_raw(body)
        raise ValueError("Did not match any expected type")
