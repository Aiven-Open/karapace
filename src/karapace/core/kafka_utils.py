"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from .config import Config
from collections.abc import Iterator
from karapace.core.kafka.admin import KafkaAdminClient
from karapace.core.kafka.consumer import KafkaConsumer
from karapace.core.kafka.producer import KafkaProducer

import contextlib


def get_oauth_token_provider(config: Config) -> object | None:
    """Return the configured OAuth token provider instance, if any.

    The provider is instantiated once during Config initialization and
    validated to expose a ``token_with_expiry`` method as required by
    confluent-kafka's OAUTHBEARER flow.
    """
    return config._sasl_oauth_token_provider


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
