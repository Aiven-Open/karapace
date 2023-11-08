"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from .config import Config
from .utils import KarapaceKafkaClient
from kafka import KafkaConsumer, KafkaProducer
from karapace.kafka_admin import KafkaAdminClient
from typing import Iterator

import contextlib


def kafka_admin_from_config(config: Config) -> KafkaAdminClient:
    return KafkaAdminClient(
        bootstrap_servers=config["bootstrap_uri"],
        client_id=config["client_id"],
        security_protocol=config["security_protocol"],
        sasl_mechanism=config["sasl_mechanism"],
        sasl_plain_username=config["sasl_plain_username"],
        sasl_plain_password=config["sasl_plain_password"],
        ssl_cafile=config["ssl_cafile"],
        ssl_certfile=config["ssl_certfile"],
        ssl_keyfile=config["ssl_keyfile"],
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
        retries=0,
    )
    try:
        yield producer
    finally:
        try:
            producer.flush()
        finally:
            producer.close()
