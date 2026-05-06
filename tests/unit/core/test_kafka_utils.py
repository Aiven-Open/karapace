"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from karapace.core.config import Config
from karapace.core.kafka_utils import (
    get_oauth_token_provider,
    kafka_admin_from_config,
    kafka_consumer_from_config,
    kafka_producer_from_config,
)
from unittest.mock import MagicMock, patch


def _make_config() -> Config:
    return Config(
        bootstrap_uri="kafka-host:9092",
        client_id="test-client",
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username="user",
        sasl_plain_password="pass",
        ssl_cafile="/tmp/ca",
        ssl_certfile="/tmp/cert",
        ssl_keyfile="/tmp/key",
        session_timeout_ms=12345,
        metadata_max_age_ms=54321,
    )


def test_get_oauth_token_provider_none_by_default() -> None:
    assert get_oauth_token_provider(Config()) is None


def test_get_oauth_token_provider_returns_configured_instance() -> None:
    config = Config()
    sentinel = object()
    config._sasl_oauth_token_provider = sentinel
    assert get_oauth_token_provider(config) is sentinel


@patch("karapace.core.kafka_utils.KafkaAdminClient")
def test_kafka_admin_from_config_passes_all_kwargs(admin_cls: MagicMock) -> None:
    config = _make_config()

    admin = kafka_admin_from_config(config)

    assert admin is admin_cls.return_value
    admin_cls.assert_called_once_with(
        bootstrap_servers="kafka-host:9092",
        client_id="test-client",
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username="user",
        sasl_plain_password="pass",
        ssl_cafile="/tmp/ca",
        ssl_certfile="/tmp/cert",
        ssl_keyfile="/tmp/key",
    )
    assert "sasl_oauth_token_provider" not in admin_cls.call_args.kwargs


@patch("karapace.core.kafka_utils.KafkaAdminClient")
def test_kafka_admin_from_config_adds_oauth_token_provider(admin_cls: MagicMock) -> None:
    config = Config()
    provider = object()
    config._sasl_oauth_token_provider = provider

    kafka_admin_from_config(config)

    assert admin_cls.call_args.kwargs["sasl_oauth_token_provider"] is provider


@patch("karapace.core.kafka_utils.KafkaConsumer")
def test_kafka_consumer_from_config_yields_and_closes(consumer_cls: MagicMock) -> None:
    config = _make_config()
    consumer_instance = consumer_cls.return_value

    with kafka_consumer_from_config(config, topic="my-topic") as consumer:
        assert consumer is consumer_instance
        consumer_instance.close.assert_not_called()

    consumer_instance.close.assert_called_once()
    kwargs = consumer_cls.call_args.kwargs
    assert kwargs["topic"] == "my-topic"
    assert kwargs["bootstrap_servers"] == "kafka-host:9092"
    assert kwargs["enable_auto_commit"] is False
    assert kwargs["auto_offset_reset"] == "earliest"
    assert kwargs["session_timeout_ms"] == 12345
    assert kwargs["metadata_max_age_ms"] == 54321
    assert "sasl_oauth_token_provider" not in kwargs


@patch("karapace.core.kafka_utils.KafkaConsumer")
def test_kafka_consumer_from_config_closes_on_exception(consumer_cls: MagicMock) -> None:
    consumer_instance = consumer_cls.return_value

    try:
        with kafka_consumer_from_config(Config(), topic="t"):
            raise RuntimeError("boom")
    except RuntimeError:
        pass

    consumer_instance.close.assert_called_once()


@patch("karapace.core.kafka_utils.KafkaConsumer")
def test_kafka_consumer_from_config_adds_oauth_token_provider(consumer_cls: MagicMock) -> None:
    config = Config()
    provider = object()
    config._sasl_oauth_token_provider = provider

    with kafka_consumer_from_config(config, topic="t"):
        pass

    assert consumer_cls.call_args.kwargs["sasl_oauth_token_provider"] is provider


@patch("karapace.core.kafka_utils.KafkaProducer")
def test_kafka_producer_from_config_yields_and_flushes(producer_cls: MagicMock) -> None:
    config = _make_config()
    producer_instance = producer_cls.return_value

    with kafka_producer_from_config(config) as producer:
        assert producer is producer_instance
        producer_instance.flush.assert_not_called()

    producer_instance.flush.assert_called_once()
    kwargs = producer_cls.call_args.kwargs
    assert kwargs["bootstrap_servers"] == "kafka-host:9092"
    assert kwargs["retries"] == 0
    assert kwargs["session_timeout_ms"] == 12345
    assert "sasl_oauth_token_provider" not in kwargs


@patch("karapace.core.kafka_utils.KafkaProducer")
def test_kafka_producer_from_config_flushes_on_exception(producer_cls: MagicMock) -> None:
    producer_instance = producer_cls.return_value

    try:
        with kafka_producer_from_config(Config()):
            raise RuntimeError("boom")
    except RuntimeError:
        pass

    producer_instance.flush.assert_called_once()


@patch("karapace.core.kafka_utils.KafkaProducer")
def test_kafka_producer_from_config_adds_oauth_token_provider(producer_cls: MagicMock) -> None:
    config = Config()
    provider = object()
    config._sasl_oauth_token_provider = provider

    with kafka_producer_from_config(config):
        pass

    assert producer_cls.call_args.kwargs["sasl_oauth_token_provider"] is provider
