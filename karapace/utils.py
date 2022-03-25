"""
karapace - utils

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from functools import partial
from http import HTTPStatus
from kafka.client_async import BrokerConnection, KafkaClient, MetadataRequest
from typing import NoReturn, Optional
from urllib.parse import urljoin

import aiohttp
import datetime
import decimal
import json as jsonlib
import kafka.client_async
import logging
import requests
import ssl
import time
import types

log = logging.getLogger("KarapaceUtils")
NS_BLACKOUT_DURATION_SECONDS = 120


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
        default=default_json_serialization,
    )
    return res.encode("utf-8") if binary else res


def assert_never(value: NoReturn) -> NoReturn:
    raise RuntimeError(f"This code should never be reached, got: {value}")


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


async def get_aiohttp_client() -> aiohttp.ClientSession:
    return aiohttp.ClientSession()


class Client:
    def __init__(self, server_uri=None, client_factory=get_aiohttp_client, server_ca: Optional[str] = None):
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


def convert_to_int(object_: dict, key: str, content_type: str):
    if object_.get(key) is None:
        return
    try:
        object_[key] = int(object_[key])
    except ValueError:
        from karapace.rapu import http_error

        http_error(
            message=f"{key} is not a valid int: {object_[key]}",
            content_type=content_type,
            code=HTTPStatus.INTERNAL_SERVER_ERROR,
        )


class KarapaceKafkaClient(KafkaClient):
    def __init__(self, **configs):
        kafka.client_async.BrokerConnection = KarapaceBrokerConnection
        super().__init__(**configs)

    def close_invalid_connections(self):
        update_needed = False
        with self._lock:
            conns = self._conns.copy().values()
            for conn in conns:
                if conn and conn.ns_blackout():
                    log.info(
                        "Node id %s no longer in cluster metadata, closing connection and requesting update", conn.node_id
                    )
                    self.close(conn.node_id)
                    update_needed = True
        if update_needed:
            self.cluster.request_update()

    def _poll(self, timeout):
        super()._poll(timeout)
        try:
            self.close_invalid_connections()
        except Exception as e:  # pylint: disable=broad-except
            log.error("Error closing invalid connections: %r", e)

    def _maybe_refresh_metadata(self, wakeup=False):
        """
        Lifted from the parent class with the caveat that the node id will always belong to the bootstrap node,
        thus ensuring we do not end up in a stale metadata loop
        """
        ttl = self.cluster.ttl()
        wait_for_in_progress_ms = self.config["request_timeout_ms"] if self._metadata_refresh_in_progress else 0
        metadata_timeout = max(ttl, wait_for_in_progress_ms)

        if metadata_timeout > 0:
            return metadata_timeout
        # pylint: disable=protected-access
        bootstrap_nodes = list(self.cluster._bootstrap_brokers.values())
        # pylint: enable=protected-access
        if not bootstrap_nodes:
            node_id = self.least_loaded_node()
        else:
            node_id = bootstrap_nodes[0].nodeId
        if node_id is None:
            log.debug("Give up sending metadata request since no node is available")
            return self.config["reconnect_backoff_ms"]

        if self._can_send_request(node_id):
            topics = list(self._topics)
            if not topics and self.cluster.is_bootstrap(node_id):
                topics = list(self.config["bootstrap_topics_filter"])

            if self.cluster.need_all_topic_metadata or not topics:
                topics = [] if self.config["api_version"] < (0, 10) else None
            api_version = 0 if self.config["api_version"] < (0, 10) else 1
            request = MetadataRequest[api_version](topics)
            log.debug("Sending metadata request %s to node %s", request, node_id)
            future = self.send(node_id, request, wakeup=wakeup)
            future.add_callback(self.cluster.update_metadata)
            future.add_errback(self.cluster.failed_update)

            self._metadata_refresh_in_progress = True

            # pylint: disable=unused-argument
            def refresh_done(val_or_error):
                self._metadata_refresh_in_progress = False

            # pylint: enable=unused-argument

            future.add_callback(refresh_done)
            future.add_errback(refresh_done)
            return self.config["request_timeout_ms"]
        if self._connecting:
            return self.config["reconnect_backoff_ms"]

        if self.maybe_connect(node_id, wakeup=wakeup):
            log.debug("Initializing connection to node %s for metadata request", node_id)
            return self.config["reconnect_backoff_ms"]
        return float("inf")


class KarapaceBrokerConnection(BrokerConnection):
    def __init__(self, host, port, afi, **configs):
        super().__init__(host, port, afi, **configs)
        self.error = None
        self.fail_time = time.monotonic()

    def close(self, error=None):
        self.error = error
        self.fail_time = time.monotonic()
        super().close(error)

    def ns_blackout(self):
        return "DNS failure" in str(self.error) and self.fail_time + NS_BLACKOUT_DURATION_SECONDS > time.monotonic()

    def blacked_out(self):
        return self.ns_blackout() or super().blacked_out()
