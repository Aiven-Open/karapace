"""
karapace - utils

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from http import HTTPStatus
from kafka.client_async import BrokerConnection, KafkaClient, MetadataRequest
from types import MappingProxyType
from typing import NoReturn, overload, Union

import json as jsonlib
import kafka.client_async
import logging
import time

log = logging.getLogger("KarapaceUtils")
NS_BLACKOUT_DURATION_SECONDS = 120


def _isoformat(datetime_obj: datetime) -> str:
    """Return datetime to ISO 8601 variant suitable for users.

    Assume UTC for datetime objects without a timezone, always use the Z
    timezone designator.
    """
    if datetime_obj.tzinfo:
        datetime_obj = datetime_obj.astimezone(timezone.utc).replace(tzinfo=None)
    return datetime_obj.isoformat() + "Z"


@overload
def default_json_serialization(obj: datetime) -> str:
    ...


@overload
def default_json_serialization(obj: timedelta) -> float:
    ...


@overload
def default_json_serialization(obj: Decimal) -> str:
    ...


@overload
def default_json_serialization(obj: MappingProxyType) -> dict:
    ...


def default_json_serialization(  # pylint: disable=inconsistent-return-statements
    obj: Union[datetime, timedelta, Decimal, MappingProxyType],
) -> Union[str, float, dict]:
    if isinstance(obj, datetime):
        return _isoformat(obj)
    if isinstance(obj, timedelta):
        return obj.total_seconds()
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, MappingProxyType):
        return dict(obj)

    assert_never("Object of type {!r} is not JSON serializable".format(obj.__class__.__name__))


def json_encode(obj, *, sort_keys: bool = True, binary=False):
    res = jsonlib.dumps(
        obj,
        sort_keys=sort_keys,
        indent=None,
        separators=(",", ":"),
        default=default_json_serialization,
    )
    return res.encode("utf-8") if binary else res


def assert_never(value: NoReturn) -> NoReturn:
    raise RuntimeError(f"This code should never be reached, got: {value}")


class Timeout(Exception):
    pass


@dataclass(frozen=True)
class Expiration:
    start_time: float
    deadline: float

    @classmethod
    def from_timeout(cls, timeout: float) -> "Expiration":
        start_time = time.monotonic()
        deadline = start_time + timeout
        return cls(start_time, deadline)

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.start_time

    def is_expired(self) -> bool:
        return time.monotonic() > self.deadline

    def raise_timeout_if_expired(self, msg_format: str, *args: object, **kwargs: object) -> None:
        """Raise `Timeout` if this object is expired.

        Note:
            This method is supposed to be used in a loop, e.g.:

                expiration = Expiration.from_timeout(timeout=60)
                while is_data_ready(data):
                    expiration.raise_timeout_if_expired("something about", data)
                    data = gather_data()

            The exception message should be meaningful, so it may format data
            into the message itself. However formatting is expensive and should
            be done only when the deadline is expired, so this uses a similar
            interface to `logging.<level>()`.
        """
        if self.is_expired():
            raise Timeout(msg_format.format(*args, **kwargs))


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
