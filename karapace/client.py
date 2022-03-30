"""
karapace - utils

Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from functools import partial
from typing import Optional
from urllib.parse import urljoin

import aiohttp
import logging
import requests
import ssl

log = logging.getLogger(__name__)


async def _get_aiohttp_client() -> aiohttp.ClientSession:
    return aiohttp.ClientSession()


class Result:
    def __init__(self, status, json_result, headers=None):
        # We create both status and status_code so people can be agnostic on whichever to use
        self.status_code = status
        self.status = status
        self.json_result = json_result
        self.headers = headers if headers else {}

    def json(self):
        return self.json_result

    def __repr__(self):
        return "Result(status=%d, json_result=%r)" % (self.status_code, self.json_result)

    @property
    def ok(self):
        return 200 <= self.status_code < 300


class Client:
    def __init__(self, server_uri=None, client_factory=_get_aiohttp_client, server_ca: Optional[str] = None):
        self.server_uri = server_uri or ""
        self.path_for = partial(urljoin, self.server_uri)
        self.session = requests.Session()
        self.session.verify = server_ca
        # aiohttp requires to be in the same async loop when creating its client and when using it.
        # Since karapace Client object is initialized before creating the async context, (in
        # kafka_rest_api main, when KafkaRest is created), we can't create the aiohttp here.
        # Instead we wait for the first query in async context and lazy-initialize aiohttp client.
        self.client_factory = client_factory
        if server_ca is None:
            self.ssl_mode = False
        else:
            self.ssl_mode = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            self.ssl_mode.load_verify_locations(cafile=server_ca)
        self._client = None

    async def close(self):
        try:
            self.session.close()
        except:  # pylint: disable=bare-except
            log.info("Could not close session")
        try:
            if self._client is not None:
                await self._client.close()
        except:  # pylint: disable=bare-except
            log.info("Could not close client")

    async def get_client(self):
        if self._client is None:
            self._client = await self.client_factory()
        return self._client

    async def get(self, path, json=None, headers=None):
        path = self.path_for(path)
        if not headers:
            headers = {}
        client = await self.get_client()
        if client:
            async with client.get(
                path,
                json=json,
                headers=headers,
                ssl=self.ssl_mode,
            ) as res:
                # required for forcing the response body conversion to json despite missing valid Accept headers
                json_result = await res.json(content_type=None)
                return Result(res.status, json_result, headers=res.headers)
        elif self.server_uri:
            res = requests.get(path, headers=headers, json=json)
            return Result(status=res.status_code, json_result=res.json(), headers=res.headers)

    async def delete(self, path, headers=None):
        path = self.path_for(path)
        if not headers:
            headers = {}
        client = await self.get_client()
        if client:
            async with client.delete(
                path,
                headers=headers,
                ssl=self.ssl_mode,
            ) as res:
                json_result = {} if res.status == 204 else await res.json()
                return Result(res.status, json_result, headers=res.headers)
        elif self.server_uri:
            res = requests.delete(path, headers=headers)
            json_result = {} if res.status_code == 204 else res.json()
            return Result(status=res.status_code, json_result=json_result, headers=res.headers)

    async def post(self, path, json, headers=None):
        path = self.path_for(path)
        if not headers:
            headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

        client = await self.get_client()
        if client:
            async with client.post(
                path,
                headers=headers,
                json=json,
                ssl=self.ssl_mode,
            ) as res:
                json_result = {} if res.status == 204 else await res.json()
                return Result(res.status, json_result, headers=res.headers)
        elif self.server_uri:
            res = self.session.post(path, headers=headers, json=json)
            json_body = {} if res.status_code == 204 else res.json()
            return Result(status=res.status_code, json_result=json_body, headers=res.headers)

    async def put(self, path, json, headers=None):
        path = self.path_for(path)
        if not headers:
            headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

        client = await self.get_client()
        if client:
            async with client.put(
                path,
                headers=headers,
                json=json,
                ssl=self.ssl_mode,
            ) as res:
                json_result = await res.json()
                return Result(res.status, json_result, headers=res.headers)
        elif self.server_uri:
            res = self.session.put(path, headers=headers, json=json)
            return Result(status=res.status_code, json_result=res.json(), headers=res.headers)

    async def put_with_data(self, path, data, headers):
        path = self.path_for(path)
        client = await self.get_client()
        if client:
            async with client.put(
                path,
                headers=headers,
                data=data,
                ssl=self.ssl_mode,
            ) as res:
                json_result = await res.json()
                return Result(res.status, json_result, headers=res.headers)
        elif self.server_uri:
            res = self.session.put(path, headers=headers, data=data)
            return Result(status=res.status_code, json_result=res.json(), headers=res.headers)
