"""
karapace - utils

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from aiohttp import BasicAuth, ClientSession
from collections.abc import Awaitable, Callable, Mapping
from karapace.typing import JsonData
from urllib.parse import urljoin

import logging
import ssl

Path = str
Headers = dict

LOG = logging.getLogger(__name__)


async def _get_aiohttp_client(*, auth: BasicAuth | None = None) -> ClientSession:
    return ClientSession(auth=auth)


class Result:
    def __init__(
        self,
        status: int,
        json_result: JsonData,
        headers: Mapping | None = None,
    ) -> None:
        self.status_code = status
        self.json_result = json_result
        self.headers = headers if headers else {}

    def json(self) -> JsonData:
        return self.json_result

    def __repr__(self) -> str:
        return "Result(status=%d, json_result=%r)" % (self.status_code, self.json_result)

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300


class Client:
    def __init__(
        self,
        server_uri: str | None = None,
        client_factory: Callable[..., Awaitable[ClientSession]] = _get_aiohttp_client,
        server_ca: str | None = None,
        session_auth: BasicAuth | None = None,
    ) -> None:
        self.server_uri = server_uri or ""
        self.session_auth = session_auth
        # aiohttp requires to be in the same async loop when creating its client and when using it.
        # Since karapace Client object is initialized before creating the async context, (in
        # kafka_rest_api main, when KafkaRest is created), we can't create the aiohttp here.
        # Instead we wait for the first query in async context and lazy-initialize aiohttp client.
        self.client_factory = client_factory

        self.ssl_mode: None | bool | ssl.SSLContext
        if server_ca is None:
            self.ssl_mode = False
        else:
            self.ssl_mode = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            self.ssl_mode.load_verify_locations(cafile=server_ca)
        self._client: ClientSession | None = None

    def path_for(self, path: Path) -> str:
        return urljoin(self.server_uri, path)

    async def close(self) -> None:
        try:
            if self._client is not None:
                await self._client.close()
        except Exception:
            LOG.error("Could not close client")

    async def get_client(self) -> ClientSession:
        if self._client is None:
            self._client = await self.client_factory(auth=self.session_auth)

        return self._client

    async def get(
        self,
        path: Path,
        json: JsonData = None,
        headers: Headers | None = None,
        auth: BasicAuth | None = None,
        params: Mapping[str, str] | None = None,
        json_response: bool = True,
    ) -> Result:
        path = self.path_for(path)
        if not headers:
            headers = {}
        client = await self.get_client()
        async with client.get(
            path,
            json=json,
            headers=headers,
            auth=auth,
            ssl=self.ssl_mode,
            params=params,
        ) as res:
            # required for forcing the response body conversion to json despite missing valid Accept headers
            result = await res.json(content_type=None) if json_response else await res.text()
            return Result(res.status, result, headers=res.headers)

    async def delete(
        self,
        path: Path,
        headers: Headers | None = None,
        auth: BasicAuth | None = None,
    ) -> Result:
        path = self.path_for(path)
        if not headers:
            headers = {}
        client = await self.get_client()
        async with client.delete(
            path,
            headers=headers,
            auth=auth,
            ssl=self.ssl_mode,
        ) as res:
            json_result = {} if res.status == 204 else await res.json()
            return Result(res.status, json_result, headers=res.headers)

    async def post(
        self,
        path: Path,
        json: JsonData,
        headers: Headers | None = None,
        auth: BasicAuth | None = None,
    ) -> Result:
        path = self.path_for(path)
        if not headers:
            headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

        client = await self.get_client()
        async with client.post(
            path,
            headers=headers,
            auth=auth,
            json=json,
            ssl=self.ssl_mode,
        ) as res:
            json_result = {} if res.status == 204 else await res.json()
            return Result(res.status, json_result, headers=res.headers)

    async def put(
        self,
        path: Path,
        json: JsonData,
        headers: Headers | None = None,
        auth: BasicAuth | None = None,
    ) -> Result:
        path = self.path_for(path)
        if not headers:
            headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

        client = await self.get_client()
        async with client.put(
            path,
            headers=headers,
            auth=auth,
            json=json,
            ssl=self.ssl_mode,
        ) as res:
            json_result = await res.json()
            return Result(res.status, json_result, headers=res.headers)

    async def put_with_data(
        self,
        path: Path,
        data: JsonData,
        headers: Headers | None,
        auth: BasicAuth | None = None,
    ) -> Result:
        path = self.path_for(path)
        client = await self.get_client()
        async with client.put(
            path,
            headers=headers,
            auth=auth,
            data=data,
            ssl=self.ssl_mode,
        ) as res:
            json_result = await res.json()
            return Result(res.status, json_result, headers=res.headers)
