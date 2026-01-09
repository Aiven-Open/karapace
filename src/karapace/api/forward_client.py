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
import ssl

LOG = logging.getLogger(__name__)


BaseModelResponse = TypeVar("BaseModelResponse", bound=BaseModel)
SimpleTypeResponse = TypeVar("SimpleTypeResponse", bound=Union[int, list[int]])


class ForwardClient:
    USER_AGENT = f"Karapace/{__version__}"

    def __init__(self, config: Config) -> None:
        self.advertised_protocol = config.advertised_protocol
        self._forward_client: aiohttp.ClientSession = aiohttp.ClientSession(headers={"User-Agent": self.USER_AGENT})
        self._ssl_context: ssl.SSLContext | None = None
        if self.advertised_protocol == "https":
            self._ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
            self._ssl_context.load_verify_locations(cafile=config.server_tls_cafile)

    async def close(self) -> None:
        await self._forward_client.close()

    def _acceptable_response_content_type(self, *, content_type: str) -> bool:
        return (
            content_type.startswith("application/") and content_type.endswith("json")
        ) or content_type == "application/octet-stream"

    async def _forward_request_remote(
        self,
        *,
        request: Request,
        primary_url: str,
    ) -> tuple[str, int]:  # Return both body and status code
        LOG.info("Forwarding %s request to remote url: %r since we're not the master", request.method, request.url)
        timeout = 60.0
        func = getattr(self._forward_client, request.method.lower())

        forward_url = f"{primary_url}{request.url.path}"
        if request.url.query:
            forward_url = f"{forward_url}?{request.url.query}"

        async with async_timeout.timeout(timeout):
            body_data = await request.body()
            async with func(
                forward_url, headers=request.headers.mutablecopy(), data=body_data, ssl=self._ssl_context
            ) as response:
                body = await response.text()
                # Return body and status regardless of content type for error cases
                if not self._acceptable_response_content_type(content_type=response.headers.get("Content-Type")):
                    LOG.error(
                        "Unknown response content type for forwarded request: %s", response.headers.get("Content-Type")
                    )
                return body, response.status

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
        body, http_status = await self._forward_request_remote(request=request, primary_url=primary_url)

        # If the leader returned an error status, parse and re-raise it
        if http_status >= 400:
            try:
                error_data = json_decode(body)
            except Exception:
                error_data = {
                    "error_code": http_status,
                    "message": body if isinstance(body, str) else body.decode("utf-8", errors="replace"),
                }

            raise HTTPException(
                status_code=http_status,  # Use HTTP status, not error_code from body
                detail=error_data,
            )

        # Success case - parse according to expected response type
        if response_type is int:
            return int(body)  # type: ignore[return-value]
        if response_type == list[int]:
            return json_decode(body, assume_type=list[int])  # type: ignore[return-value]
        if issubclass(response_type, BaseModel):
            return response_type.parse_raw(body)
        raise ValueError("Did not match any expected type")
