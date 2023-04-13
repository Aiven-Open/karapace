"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from . import constants
from .config import Config
from .utils import KarapaceKafkaClient
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from typing import Iterator

import contextlib


def kafka_admin_from_config(config: Config) -> KafkaAdminClient:
    return KafkaAdminClient(
        api_version_auto_timeout_ms=constants.API_VERSION_AUTO_TIMEOUT_MS,
        bootstrap_servers=config["bootstrap_uri"],
        client_id=config["client_id"],
        security_protocol=config["security_protocol"],
        ssl_cafile=config["ssl_cafile"],
        ssl_certfile=config["ssl_certfile"],
        ssl_keyfile=config["ssl_keyfile"],
        kafka_client=KarapaceKafkaClient,
    )


@contextlib.contextmanager
def kafka_consumer_from_config(config: Config, topic: str) -> Iterator[KafkaConsumer]:
    consumer = KafkaConsumer(
        topic,
        enable_auto_commit=False,
        bootstrap_servers=config["bootstrap_uri"],
        client_id=config["client_id"],
        security_protocol=config["security_protocol"],
        ssl_cafile=config["ssl_cafile"],
        ssl_certfile=config["ssl_certfile"],
        ssl_keyfile=config["ssl_keyfile"],
        sasl_mechanism=config["sasl_mechanism"],
        sasl_plain_username=config["sasl_plain_username"],
        sasl_plain_password=config["sasl_plain_password"],
        auto_offset_reset="earliest",
        metadata_max_age_ms=config["metadata_max_age_ms"],
        kafka_client=KarapaceKafkaClient,
    )
    try:
        yield consumer
    finally:
        consumer.close()


@contextlib.contextmanager
def kafka_producer_from_config(config: Config) -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers=config["bootstrap_uri"],
        security_protocol=config["security_protocol"],
        ssl_cafile=config["ssl_cafile"],
        ssl_certfile=config["ssl_certfile"],
        ssl_keyfile=config["ssl_keyfile"],
        sasl_mechanism=config["sasl_mechanism"],
        sasl_plain_username=config["sasl_plain_username"],
        sasl_plain_password=config["sasl_plain_password"],
        kafka_client=KarapaceKafkaClient,
    )
    try:
        yield producer
    finally:
        try:
            producer.flush()
        finally:
            producer.close()
