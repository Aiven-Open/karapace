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


def kafka_admin_from_config(config: Config) -> KafkaAdminClient:
    return KafkaAdminClient(
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


@contextlib.contextmanager
def kafka_consumer_from_config(config: Config, topic: str) -> Iterator[KafkaConsumer]:
    consumer = KafkaConsumer(
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
    try:
        yield consumer
    finally:
        consumer.close()


@contextlib.contextmanager
def kafka_producer_from_config(config: Config) -> Iterator[KafkaProducer]:
    producer = KafkaProducer(
        bootstrap_servers=config.bootstrap_uri,
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
    try:
        yield producer
    finally:
        producer.flush()
