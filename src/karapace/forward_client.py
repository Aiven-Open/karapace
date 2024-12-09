"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from karapace.version import __version__

import aiohttp
import async_timeout
import logging

LOG = logging.getLogger(__name__)


class ForwardClient:
    USER_AGENT = f"Karapace/{__version__}"

    def __init__(self):
        self._forward_client: aiohttp.ClientSession | None = None

    def _get_forward_client(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(headers={"User-Agent": ForwardClient.USER_AGENT})

    async def forward_request_remote(self, *, request: Request, primary_url: str) -> Response:
        LOG.info("Forwarding %s request to remote url: %r since we're not the master", request.method, request.url)
        timeout = 60.0
        headers = request.headers.mutablecopy()
        func = getattr(self._get_forward_client(), request.method.lower())
        # auth_header = request.headers.get("Authorization")
        # if auth_header is not None:
        #    headers["Authorization"] = auth_header

        forward_url = f"{primary_url}{request.url.path}"
        if request.url.query:
            forward_url = f"{forward_url}?{request.url.query}"
        logging.error(forward_url)

        with async_timeout.timeout(timeout):
            body_data = await request.body()
            async with func(forward_url, headers=headers, data=body_data) as response:
                if response.headers.get("Content-Type", "").startswith("application/json"):
                    return JSONResponse(
                        content=await response.text(),
                        status_code=response.status,
                        headers=response.headers,
                    )
                return PlainTextResponse(
                    content=await response.text(),
                    status_code=response.status,
                    headers=response.headers,
                )
