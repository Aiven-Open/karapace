"""
karapace - utils

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from typing import Literal
from aiohttp import BasicAuth, ClientSession
from collections.abc import Awaitable, Callable, Mapping
from karapace.core.typing import JsonData
from urllib.parse import urljoin, quote_plus

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
        params: Mapping[str, str] | None = None,
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
            params=params,
        ) as res:
            json_result = {} if res.status == 204 else await res.json()
            return Result(res.status, json_result, headers=res.headers)

    async def post(
        self,
        path: Path,
        json: JsonData,
        headers: Headers | None = None,
        auth: BasicAuth | None = None,
        params: Mapping[str, str] | None = None,
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
            params=params,
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

    # Per resource functions
    # COMPATIBILITY
    async def post_compatibility_subject_version(
        self, *, subject: str, version: int | Literal["latest"], json: JsonData
    ) -> Result:
        return await self.post(
            path=f"compatibility/subjects/{quote_plus(subject)}/versions/{version}",
            json=json,
        )

    # CONFIG
    async def get_config(self) -> Result:
        return await self.get(path="/config")

    async def put_config(self, *, json: JsonData) -> Result:
        return await self.put(path="/config", json=json)

    async def get_config_subject(self, *, subject: str, defaultToGlobal: bool | None = None) -> Result:
        path = f"/config/{quote_plus(subject)}"
        if defaultToGlobal is not None:
            path = f"{path}?defaultToGlobal={str(defaultToGlobal).lower()}"
        return await self.get(path=path)

    async def put_config_subject(self, *, subject: str, json: JsonData) -> Result:
        return await self.put(path=f"config/{quote_plus(subject)}", json=json)

    async def delete_config_subject(self, *, subject: str) -> Result:
        return await self.delete(path=f"config/{quote_plus(subject)}")

    # MODE
    async def get_mode(self) -> Result:
        return await self.get(path="/mode")

    async def get_mode_subject(self, *, subject: str) -> Result:
        return await self.get(path=f"/mode/{quote_plus(subject)}")

    # SCHEMAS
    async def get_schemas(self) -> Result:
        return await self.get("/schemas")

    async def get_types(self) -> Result:
        return await self.get(path="/schemas/types")

    async def get_schema_by_id(self, *, schema_id: int, params: Mapping[str, str] | None = None) -> Result:
        return await self.get(path=f"/schemas/ids/{schema_id}", params=params)

    async def get_schema_by_id_versions(self, *, schema_id: int, params: Mapping[str, str] | None = None) -> Result:
        return await self.get(path=f"/schemas/ids/{schema_id}/versions", params=params)

    # SUBJECTS
    async def get_subjects(self, *, params: Mapping[str, str] | None = None) -> Result:
        return await self.get("/subjects", params=params)

    async def get_subjects_versions(self, *, subject: str) -> Result:
        return await self.get(f"subjects/{quote_plus(subject)}/versions")

    async def post_subjects(self, *, subject: str, json: JsonData, params: Mapping[str, str] | None = None) -> Result:
        return await self.post(f"/subjects/{quote_plus(subject)}", json=json, params=params)

    async def post_subjects_versions(
        self, *, subject: str, json: JsonData, params: Mapping[str, str] | None = None
    ) -> Result:
        return await self.post(f"/subjects/{quote_plus(subject)}/versions", json=json, params=params)

    async def get_subjects_subject_version(
        self, *, subject: str, version: int | Literal["latest"], params: Mapping[str, str] | None = None
    ) -> Result:
        return await self.get(f"/subjects/{quote_plus(subject)}/versions/{version}", params=params)

    async def get_subjects_subject_version_schema(self, *, subject: str, version: int | Literal["latest"]) -> Result:
        return await self.get(f"subjects/{quote_plus(subject)}/versions/{version}/schema")

    async def get_subjects_subject_version_referenced_by(self, *, subject: str, version: int | Literal["latest"]) -> Result:
        return await self.get(f"subjects/{quote_plus(subject)}/versions/{version}/referencedby")

    async def delete_subjects(self, *, subject: str, params: Mapping[str, str] | None = None) -> Result:
        return await self.delete(path=f"/subjects/{quote_plus(subject)}", params=params)

    async def delete_subjects_version(
        self, *, subject: str, version: int | Literal["latest"], permanent: bool | None = None
    ) -> Result:
        path = f"subjects/{quote_plus(subject)}/versions/{version}"
        if permanent is not None:
            path = f"{path}?permanent={str(permanent).lower()}"
        return await self.delete(path)
