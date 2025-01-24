"""
karapace - Test rest client with retries

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from aiohttp import BasicAuth
from collections.abc import Mapping
from karapace.core.client import Client, Headers, Path, Result
from karapace.core.typing import JsonData
from tenacity import retry, stop_after_attempt, wait_fixed
from typing import Final

RETRY_WAIT_SECONDS: Final = 0.5


class UnexpectedResponseStatus(Exception):
    pass


class RetryRestClient:
    def __init__(self, client: Client):
        self._karapace_client = client

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(RETRY_WAIT_SECONDS))
    async def get(
        self,
        path: Path,
        json: JsonData | None = None,
        headers: Headers | None = None,
        auth: BasicAuth | None = None,
        params: Mapping[str, str] | None = None,
        expected_response_code: int = 200,
    ) -> Result:
        response: Result = await self._karapace_client.get(path, headers=headers, json=json, auth=auth, params=params)
        if response.status_code != expected_response_code:
            raise UnexpectedResponseStatus(f"Unexpected status code: {response!r}")
        return response

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(RETRY_WAIT_SECONDS))
    async def delete(
        self,
        path: Path,
        headers: Headers | None = None,
        auth: BasicAuth | None = None,
        expected_response_code: int = 200,
    ) -> Result:
        response: Result = await self._karapace_client.delete(path=path, headers=headers, auth=auth)
        if response.status_code != expected_response_code:
            raise UnexpectedResponseStatus(f"Unexpected status code: {response!r}")
        return response

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(RETRY_WAIT_SECONDS))
    async def post(
        self,
        path: Path,
        json: JsonData,
        headers: Headers | None = None,
        auth: BasicAuth | None = None,
        expected_response_code: int = 200,
    ) -> Result:
        response: Result = await self._karapace_client.post(path=path, json=json, headers=headers, auth=auth)
        if response.status_code != expected_response_code:
            raise UnexpectedResponseStatus(f"Unexpected status code: {response!r}")
        return response

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(RETRY_WAIT_SECONDS))
    async def put(
        self,
        path: Path,
        json: JsonData,
        headers: Headers | None = None,
        auth: BasicAuth | None = None,
        expected_response_code: int = 200,
    ) -> Result:
        response: Result = await self._karapace_client.put(path=path, json=json, headers=headers, auth=auth)
        if response.status_code != expected_response_code:
            raise UnexpectedResponseStatus(f"Unexpected status code: {response!r}")
        return response
