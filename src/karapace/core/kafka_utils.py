"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from .config import Config
from collections.abc import Iterator
from karapace.core.kafka.admin import KafkaAdminClient
from karapace.core.kafka.common import TokenWithExpiryProvider
from karapace.core.kafka.consumer import KafkaConsumer
from karapace.core.kafka.producer import KafkaProducer
from typing import cast

import contextlib
import dataclasses
import jwt


def get_expiration_timestamp_from_jwt(token: str) -> int | None:
    # Unverified decode, only to read `exp`; Kafka validates the token itself.
    return jwt.decode(token, options={"verify_signature": False}).get("exp")


@dataclasses.dataclass
class SimpleOauthTokenProvider:
    """A pass-through OAuth token provider for a static token."""

    _token: str = dataclasses.field(repr=False)

    def token_with_expiry(self, _config: str | None = None) -> tuple[str, int | None]:
        return (self._token, get_expiration_timestamp_from_jwt(self._token))


def get_oauth_token_provider(config: Config) -> TokenWithExpiryProvider | None:
    """Return the OAuth token provider for Kafka clients, if any.

    The `sasl_oauth_token_provider_class` instance wins; otherwise a static
    `sasl_oauth_token` is used when `sasl_mechanism` is OAUTHBEARER.
    """
    provider = cast(TokenWithExpiryProvider | None, config._sasl_oauth_token_provider)
    if provider is None and config.sasl_mechanism == "OAUTHBEARER" and config.sasl_oauth_token is not None:
        return SimpleOauthTokenProvider(config.sasl_oauth_token)
    return provider


def kafka_admin_from_config(config: Config) -> KafkaAdminClient:
    kwargs: dict = dict(
        bootstrap_servers=config.bootstrap_uri,
        client_id=config.client_id,
        security_protocol=config.security_protocol,
        sasl_mechanism=config.sasl_mechanism,
        sasl_plain_username=config.sasl_plain_username,
        sasl_plain_password=config.sasl_plain_password,
        ssl_cafile=config.ssl_cafile,
        ssl_certfile=config.ssl_certfile,
        ssl_keyfile=config.ssl_keyfile,
        debug=config.librdkafka_debug,
    )
    token_provider = get_oauth_token_provider(config)
    if token_provider is not None:
        kwargs["sasl_oauth_token_provider"] = token_provider
    return KafkaAdminClient(**kwargs)


@contextlib.contextmanager
def kafka_consumer_from_config(config: Config, topic: str) -> Iterator[KafkaConsumer]:
    kwargs: dict = dict(
        bootstrap_servers=config.bootstrap_uri,
        topic=topic,
        enable_auto_commit=False,
        client_id=config.client_id,
        security_protocol=config.security_protocol,
        ssl_cafile=config.ssl_cafile,
        ssl_certfile=config.ssl_certfile,
        ssl_keyfile=config.ssl_keyfile,
        debug=config.librdkafka_debug,
        sasl_mechanism=config.sasl_mechanism,
        sasl_plain_username=config.sasl_plain_username,
        sasl_plain_password=config.sasl_plain_password,
        auto_offset_reset="earliest",
        session_timeout_ms=config.session_timeout_ms,
        metadata_max_age_ms=config.metadata_max_age_ms,
    )
    token_provider = get_oauth_token_provider(config)
    if token_provider is not None:
        kwargs["sasl_oauth_token_provider"] = token_provider
    consumer = KafkaConsumer(**kwargs)
    try:
        yield consumer
    finally:
        consumer.close()


@contextlib.contextmanager
def kafka_producer_from_config(config: Config) -> Iterator[KafkaProducer]:
    kwargs: dict = dict(
        bootstrap_servers=config.bootstrap_uri,
        client_id=config.client_id,
        security_protocol=config.security_protocol,
        ssl_cafile=config.ssl_cafile,
        ssl_certfile=config.ssl_certfile,
        ssl_keyfile=config.ssl_keyfile,
        debug=config.librdkafka_debug,
        sasl_mechanism=config.sasl_mechanism,
        sasl_plain_username=config.sasl_plain_username,
        sasl_plain_password=config.sasl_plain_password,
        retries=0,
        session_timeout_ms=config.session_timeout_ms,
    )
    token_provider = get_oauth_token_provider(config)
    if token_provider is not None:
        kwargs["sasl_oauth_token_provider"] = token_provider
    producer = KafkaProducer(**kwargs)
    try:
        yield producer
    finally:
        producer.flush()
