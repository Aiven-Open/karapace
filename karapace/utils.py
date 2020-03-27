"""
karapace - utils

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from functools import partial
from urllib.parse import urljoin

import aiohttp
import asyncio
import datetime
import decimal
import json as jsonlib
import logging
import types

log = logging.getLogger(__name__)


def isoformat(datetime_obj=None, *, preserve_subsecond=False, compact=False):
    """Return datetime to ISO 8601 variant suitable for users.
    Assume UTC for datetime objects without a timezone, always use
    the Z timezone designator."""
    if datetime_obj is None:
        datetime_obj = datetime.datetime.utcnow()
    elif datetime_obj.tzinfo:
        datetime_obj = datetime_obj.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    isof = datetime_obj.isoformat()
    if not preserve_subsecond:
        isof = isof[:19]
    if compact:
        isof = isof.replace("-", "").replace(":", "").replace(".", "")
    return isof + "Z"


def default_json_serialization(obj):
    if isinstance(obj, datetime.datetime):
        return isoformat(obj, preserve_subsecond=True)
    if isinstance(obj, datetime.timedelta):
        return obj.total_seconds()
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    if isinstance(obj, types.MappingProxyType):
        return dict(obj)

    raise TypeError("Object of type {!r} is not JSON serializable".format(obj.__class__.__name__))


def json_encode(obj, *, compact=True, sort_keys=True, binary=False):
    res = jsonlib.dumps(
        obj,
        sort_keys=sort_keys if sort_keys is not None else not compact,
        indent=None if compact else 4,
        separators=(",", ":") if compact else None,
        default=default_json_serialization
    )
    return res.encode("utf-8") if binary else res


class Result:
    def __init__(self, status, json_result, headers=None):
        # We create both status and status_code so people can be agnostic on whichever to use
        self.status_code = status
        self.status = status
        self.json_result = json_result
        self.headers = headers if headers else {}

    def json(self):
        return self.json_result


class Client:
    def __init__(self, server_uri):
        self.base_uri_builder = partial(urljoin, server_uri)
        self.session = aiohttp.ClientSession()
        self.headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

    async def post(self, path, json):
        return await self.upsert("POST", path, json)

    async def put(self, path, json):
        return await self.upsert("PUT", path, json)

    async def get(self, path):
        return await self.read_or_delete("GET", path)

    async def delete(self, path):
        return await self.read_or_delete("DELETE", path)

    async def read_or_delete(self, method, path):
        uri = self.base_uri_builder(path)
        if method == "DELETE":
            caller = self.session.delete
        else:
            caller = self.session.get
        log.debug("Running %r request for %r", method, uri)
        async with caller(
            uri,
            headers=self.headers,
        ) as res:
            json_result = await res.json()
            return Result(res.status, json_result, headers=res.headers)

    async def upsert(self, method, path, json):
        uri = self.base_uri_builder(path)
        if method == "POST":
            caller = self.session.post
        else:
            caller = self.session.put
        log.debug("Running %r request for %r", method, uri)
        async with caller(
            uri,
            headers=self.headers,
            json=json,
        ) as res:
            json_result = await res.json()
            return Result(res.status, json_result, headers=res.headers)

    def close(self):
        asyncio.get_event_loop().run_until_complete(self.session.close())
