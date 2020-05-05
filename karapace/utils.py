"""
karapace - utils

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
import datetime
import decimal
import json as jsonlib
import logging
import os
import requests
import types

log = logging.getLogger("KarapaceUtils")


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

    def __repr__(self):
        return "Result(status=%d, json_result=%r)" % (self.status_code, self.json_result)

    @property
    def ok(self):
        return self.status_code == 200


class Client:
    def __init__(self, server_uri=None, client=None):
        self.server_uri = server_uri
        self.session = requests.Session()
        self.client = client

    async def close(self):
        try:
            self.session.close()
        except:  # pylint: disable=bare-except
            log.exception("Could not close client")
        try:
            await self.client.close()
        except:  # pylint: disable=bare-except
            log.exception("Could not close client")

    async def get(self, path, headers=None):
        if not headers:
            headers = {}
        if self.client:
            async with self.client.get(
                path,
                headers=headers,
            ) as res:
                json_result = await res.json()
                return Result(res.status, json_result, headers=res.headers)
        elif self.server_uri:
            res = requests.get(os.path.join(self.server_uri, path), headers=headers)
            return Result(status=res.status_code, json_result=res.json(), headers=res.headers)

    async def delete(self, path, headers=None):
        if not headers:
            headers = {}
        if self.client:
            async with self.client.delete(
                path,
                headers=headers,
            ) as res:
                json_result = await res.json()
                return Result(res.status, json_result, headers=res.headers)
        elif self.server_uri:
            res = requests.delete(os.path.join(self.server_uri, path), headers=headers)
            return Result(status=res.status_code, json_result=res.json(), headers=res.headers)

    async def post(self, path, json, headers=None):
        if not headers:
            headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

        if self.client:
            async with self.client.post(
                path,
                headers=headers,
                json=json,
            ) as res:
                json_result = await res.json()
                return Result(res.status, json_result, headers=res.headers)
        elif self.server_uri:
            res = self.session.post(os.path.join(self.server_uri, path), headers=headers, json=json)
            return Result(status=res.status_code, json_result=res.json(), headers=res.headers)

    async def put(self, path, json, headers=None):
        if not headers:
            headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

        if self.client:
            async with self.client.put(
                path,
                headers=headers,
                json=json,
            ) as res:
                json_result = await res.json()
                return Result(res.status, json_result, headers=res.headers)
        elif self.server_uri:
            res = self.session.put(os.path.join(self.server_uri, path), headers=headers, json=json)
            return Result(status=res.status_code, json_result=res.json(), headers=res.headers)

    async def put_with_data(self, path, data, headers):
        if self.client:
            async with self.client.put(
                path,
                headers=headers,
                data=data,
            ) as res:
                json_result = await res.json()
                return Result(res.status, json_result, headers=res.headers)
        elif self.server_uri:
            res = self.session.put(os.path.join(self.server_uri, path), headers=headers, data=data)
            return Result(status=res.status_code, json_result=res.json(), headers=res.headers)
