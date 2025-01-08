"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from aiokafka.client import UnknownTopicOrPartitionError
from aiokafka.errors import (
    AuthenticationFailedError,
    for_code,
    IllegalStateError,
    KafkaTimeoutError,
    KafkaUnavailableError,
    NoBrokersAvailable,
)
from collections.abc import Callable, Iterable
from concurrent.futures import Future
from confluent_kafka.error import KafkaError, KafkaException
from typing import Any, Literal, NoReturn, Protocol, TypedDict, TypeVar
from typing_extensions import Unpack

import logging

T = TypeVar("T")


def single_futmap_result(futmap: dict[Any, Future[T]]) -> T:
    """Extract the result of a future wrapped in a dict.

    Bulk operations of the `confluent_kafka` library's Kafka clients return results
    wrapped in a dictionary of futures. Most often we use these bulk operations to
    operate on a single resource/entity. This function makes sure the dictionary of
    futures contains a single future and returns its result.
    """
    (future,) = futmap.values()
    return future.result()


def translate_from_kafkaerror(error: KafkaError) -> Exception:
    """Translate a `KafkaError` from `confluent_kafka` to a friendlier exception.

    `kafka.errors.for_code` is used to translate the original exception's error code
    to a domain specific error class from `aiokafka`.

    In some cases `KafkaError`s are created with error codes internal to `confluent_kafka`,
    such as various internal error codes for unknown topics or partitions:
    `_NOENT`, `_UNKNOWN_PARTITION`, `_UNKNOWN_TOPIC` - these internal errors
    have negative error codes that needs to be handled separately.
    """
    code = error.code()
    if code in (
        KafkaError._NOENT,
        KafkaError._UNKNOWN_PARTITION,
        KafkaError._UNKNOWN_TOPIC,
    ):
        return UnknownTopicOrPartitionError()
    if code == KafkaError._TIMED_OUT:
        return KafkaTimeoutError()
    if code == KafkaError._STATE:
        return IllegalStateError()
    if code == KafkaError._RESOLVE:
        return KafkaUnavailableError()

    return for_code(code)


def raise_from_kafkaexception(exc: KafkaException) -> NoReturn:
    """Raises a more developer-friendly error from a `KafkaException`.

    The `confluent_kafka` library's `KafkaException` is a wrapper around its internal
    `KafkaError`. The resulting, raised exception however is coming from
    `aiokafka`, due to these exceptions having human-readable names, providing
    better context for error handling.
    """
    raise translate_from_kafkaerror(exc.args[0]) from exc


# For now this is a bit of a trick to replace an explicit usage of
# `karapace.kafka_rest_apis.authentication.SimpleOauthTokenProvider`
# to avoid circular imports
class TokenWithExpiryProvider(Protocol):
    def token_with_expiry(self, config: str | None) -> tuple[str, int | None]: ...


class KafkaClientParams(TypedDict, total=False):
    acks: int | None
    client_id: str | None
    connections_max_idle_ms: int | None
    compression_type: str | None
    linger_ms: int | None
    message_max_bytes: int | None
    metadata_max_age_ms: int | None
    retries: int | None
    sasl_mechanism: str | None
    sasl_plain_password: str | None
    sasl_plain_username: str | None
    security_protocol: str | None
    socket_timeout_ms: int | None
    ssl_cafile: str | None
    ssl_certfile: str | None
    ssl_crlfile: str | None
    ssl_keyfile: str | None
    sasl_oauth_token_provider: TokenWithExpiryProvider
    topic_metadata_refresh_interval_ms: int | None
    # Consumer-only
    auto_offset_reset: Literal["smallest", "earliest", "beginning", "largest", "latest", "end", "error"] | None
    enable_auto_commit: bool | None
    fetch_min_bytes: int | None
    fetch_message_max_bytes: int | None
    fetch_max_wait_ms: int | None
    group_id: str | None
    session_timeout_ms: int | None


class _KafkaConfigMixin:
    """A mixin-class for Kafka client initialization.

    This mixin assumes that it'll be used in conjunction with a Kafka client
    from `confluent_kafka`, eg. `AdminClient`, `Producer`, etc. The goal is to
    extract configuration, initialization and connection verification.
    """

    def __init__(
        self,
        bootstrap_servers: Iterable[str] | str,
        verify_connection: bool = True,
        **params: Unpack[KafkaClientParams],
    ) -> None:
        self._errors: set[KafkaError] = set()
        self.log = logging.getLogger(f"{self.__module__}.{self.__class__.__qualname__}")

        super().__init__(self._get_config_from_params(bootstrap_servers, **params))  # type: ignore[call-arg]
        self._activate_callbacks()
        if verify_connection:
            self._verify_connection()

    def _get_config_from_params(self, bootstrap_servers: Iterable[str] | str, **params: Unpack[KafkaClientParams]) -> dict:
        if not isinstance(bootstrap_servers, str):
            bootstrap_servers = ",".join(bootstrap_servers)

        config: dict[str, int | str | Callable | None] = {
            "bootstrap.servers": bootstrap_servers,
            "acks": params.get("acks"),
            "client.id": params.get("client_id"),
            "connections.max.idle.ms": params.get("connections_max_idle_ms"),
            "compression.type": params.get("compression_type"),
            "linger.ms": params.get("linger_ms"),
            "message.max.bytes": params.get("message_max_bytes"),
            "metadata.max.age.ms": params.get("metadata_max_age_ms"),
            "retries": params.get("retries"),
            "sasl.mechanism": params.get("sasl_mechanism"),
            "sasl.password": params.get("sasl_plain_password"),
            "sasl.username": params.get("sasl_plain_username"),
            "security.protocol": params.get("security_protocol"),
            "socket.timeout.ms": params.get("socket_timeout_ms"),
            "ssl.ca.location": params.get("ssl_cafile"),
            "ssl.certificate.location": params.get("ssl_certfile"),
            "ssl.crl.location": params.get("ssl_crlfile"),
            "ssl.key.location": params.get("ssl_keyfile"),
            "topic.metadata.refresh.interval.ms": params.get("topic_metadata_refresh_interval_ms"),
            "error_cb": self._error_callback,
            # Consumer-only
            "auto.offset.reset": params.get("auto_offset_reset"),
            "enable.auto.commit": params.get("enable_auto_commit"),
            "fetch.min.bytes": params.get("fetch_min_bytes"),
            "fetch.message.max.bytes": params.get("fetch_message_max_bytes"),
            "fetch.wait.max.ms": params.get("fetch_max_wait_ms"),
            "group.id": params.get("group_id"),
            "session.timeout.ms": params.get("session_timeout_ms"),
        }
        config = {key: value for key, value in config.items() if value is not None}

        if "sasl_oauth_token_provider" in params:
            config["oauth_cb"] = params["sasl_oauth_token_provider"].token_with_expiry

        return config

    def _error_callback(self, error: KafkaError) -> None:
        self._errors.add(error)

    def _activate_callbacks(self) -> None:
        # Any client in the `confluent_kafka` library needs `poll` called to
        # trigger any callbacks registered (eg. for errors, OAuth tokens, etc.)
        self.poll(timeout=0.0)  # type: ignore[attr-defined]

    def _verify_connection(self) -> None:
        """Attempts to call `list_topics` a few times.

        The `list_topics` method is the only meaningful synchronous method of
        the `confluent_kafka`'s client classes that can be used to verify that
        a connection and authentication has been established with a Kafka
        cluster.

        Just instantiating and initializing the client doesn't result in
        anything in its main thread in case of errors, only error logs from another
        thread otherwise.
        """
        for _ in range(3):
            try:
                self.list_topics(timeout=1)  # type: ignore[attr-defined]
            except KafkaException as exc:
                # Other than `list_topics` throwing a `KafkaException` with an underlying
                # `KafkaError` with code `_TRANSPORT` (`-195`), if the address or port is
                # incorrect, we get no symptoms
                # Authentication errors however do show up in the errors passed
                # to the callback function defined in the `error_cb` config
                self._activate_callbacks()
                self.log.info("Could not establish connection due to errors: %s", self._errors)
                if any(error.code() == KafkaError._AUTHENTICATION for error in self._errors):
                    raise AuthenticationFailedError() from exc
                continue
            else:
                break
        else:
            raise NoBrokersAvailable()
