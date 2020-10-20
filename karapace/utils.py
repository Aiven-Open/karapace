"""
karapace - utils

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from functools import partial
from kafka.client_async import BrokerConnection, KafkaClient
from urllib.parse import urljoin

import datetime
import decimal
import json as jsonlib
import kafka.client_async
import logging
import requests
import time
import types

log = logging.getLogger("KarapaceUtils")
BLACKLIST_DURATION = 120


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
        return 200 <= self.status_code < 300


class Client:
    def __init__(self, server_uri=None, client=None):
        self.server_uri = server_uri or ""
        self.path_for = partial(urljoin, self.server_uri)
        self.session = requests.Session()
        self.client = client
        if self.client is None:
            import aiohttp
            self.client = aiohttp.ClientSession()

    async def close(self):
        try:
            self.session.close()
        except:  # pylint: disable=bare-except
            log.info("Could not close session")
        try:
            await self.client.close()
        except:  # pylint: disable=bare-except
            log.info("Could not close client")

    async def get(self, path, json=None, headers=None):
        path = self.path_for(path)
        if not headers:
            headers = {}
        if self.client:
            async with self.client.get(
                path,
                json=json,
                headers=headers,
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
        if self.client:
            async with self.client.delete(
                path,
                headers=headers,
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

        if self.client:
            async with self.client.post(
                path,
                headers=headers,
                json=json,
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

        if self.client:
            async with self.client.put(
                path,
                headers=headers,
                json=json,
            ) as res:
                json_result = await res.json()
                return Result(res.status, json_result, headers=res.headers)
        elif self.server_uri:
            res = self.session.put(path, headers=headers, json=json)
            return Result(status=res.status_code, json_result=res.json(), headers=res.headers)

    async def put_with_data(self, path, data, headers):
        path = self.path_for(path)
        if self.client:
            async with self.client.put(
                path,
                headers=headers,
                data=data,
            ) as res:
                json_result = await res.json()
                return Result(res.status, json_result, headers=res.headers)
        elif self.server_uri:
            res = self.session.put(path, headers=headers, data=data)
            return Result(status=res.status_code, json_result=res.json(), headers=res.headers)


def convert_to_int(object_: dict, key: str, content_type: str):
    if object_.get(key) is None:
        return
    try:
        object_[key] = int(object_[key])
    except ValueError:
        from karapace.rapu import http_error
        http_error(f"{key} is not a valid int: {object_[key]}", content_type, 500)


class ResolutionError(Exception):
    pass


class KarapaceKafkaClient(KafkaClient):
    def __init__(self, **configs):
        kafka.client_async.BrokerConnection = KarapaceBrokerConnection
        super().__init__(**configs)

    def _maybe_connect(self, node_id):
        with self._lock:
            log.debug("Checking blackout state for node %r", node_id)
            conn = self._conns.get(node_id)
            if conn is not None and conn.ns_blacked_out():
                log.debug("Skipping connect procedure for %r", node_id)
                return False
            log.debug("Node %r is not blacked out", node_id)
        return super()._maybe_connect(node_id)


class KarapaceBrokerConnection(BrokerConnection):
    def __init__(self, host, port, afi, **configs):
        super().__init__(host, port, afi, **configs)
        self.error = None
        self.fail_time = None

    def close(self, error=None):
        self.fail_time = time.monotonic()
        self.error = error
        super().close(error=error)

    def ns_blacked_out(self):
        now = time.monotonic()
        ns_fail_black_out = "DNS failure" in str(self.error) and now <= self.fail_time + BLACKLIST_DURATION
        log.debug(
            "Node %r is ns blacked out %r, error is %r, fail time is %r, current time is: %r",
            self.node_id, ns_fail_black_out, self.error, self.fail_time, now
        )
        return ns_fail_black_out

    def blacked_out(self):
        blacked_out = super().blacked_out()
        return blacked_out or self.ns_blacked_out()
