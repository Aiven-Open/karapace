"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Callable
from karapace.core.config import Config
from karapace.core.kafka.common import _KafkaConfigMixin
from karapace.core.kafka_utils import kafka_admin_from_config, kafka_consumer_from_config, kafka_producer_from_config
from karapace.core.key_format import KeyFormatter
from karapace.core.messaging import KarapaceProducer
from karapace.core.offset_watcher import OffsetWatcher
from karapace.core.schema_reader import _create_admin_client_from_config, _create_consumer_from_config
from karapace.core.serialization import SchemaRegistrySerializer
from karapace.kafka_rest_apis import UserRestProxy
from karapace.kafka_rest_apis.consumer_manager import ConsumerManager
from pydantic import ValidationError
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

DEBUG = "broker,metadata,topic"


def test_librdkafka_debug_is_unset_by_default() -> None:
    assert Config().librdkafka_debug is None


def test_librdkafka_debug_can_be_set_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KARAPACE_LIBRDKAFKA_DEBUG", DEBUG)

    assert Config().librdkafka_debug == DEBUG


@pytest.mark.parametrize("value", ["", "  "])
def test_blank_librdkafka_debug_is_treated_as_unset(value: str) -> None:
    # librdkafka refuses an empty `debug` value
    assert Config(librdkafka_debug=value).librdkafka_debug is None


@pytest.mark.parametrize("value", ["broker", DEBUG, "all", "broker, metadata"])
def test_valid_librdkafka_debug_is_accepted_without_creating_a_client(value: str, capfd: pytest.CaptureFixture[str]) -> None:
    assert Config(librdkafka_debug=value).librdkafka_debug == value

    # A created client would log the enabled debug contexts to stderr
    assert capfd.readouterr().err == ""


@pytest.mark.parametrize("value", ["typo", "broker,typo", "broker metadata"])
def test_invalid_librdkafka_debug_is_rejected(value: str) -> None:
    with pytest.raises(ValidationError, match='for configuration property "debug"'):
        Config(librdkafka_debug=value)


def test_invalid_librdkafka_debug_from_the_environment_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KARAPACE_LIBRDKAFKA_DEBUG", "broker,typo")

    with pytest.raises(ValidationError, match="typo"):
        Config()


def test_debug_param_is_passed_to_librdkafka() -> None:
    mixin = MagicMock(spec=_KafkaConfigMixin)

    config = _KafkaConfigMixin._get_config_from_params(mixin, "kafka-host:9092", debug=DEBUG)

    assert config["debug"] == DEBUG


def test_debug_param_is_left_out_by_default() -> None:
    mixin = MagicMock(spec=_KafkaConfigMixin)

    config = _KafkaConfigMixin._get_config_from_params(mixin, "kafka-host:9092", debug=None)

    assert "debug" not in config


def _kafka_utils_admin(config: Config) -> None:
    kafka_admin_from_config(config)


def _kafka_utils_consumer(config: Config) -> None:
    with kafka_consumer_from_config(config, topic="topic"):
        pass


def _kafka_utils_producer(config: Config) -> None:
    with kafka_producer_from_config(config):
        pass


def _karapace_producer(config: Config) -> None:
    KarapaceProducer(
        config=config, offset_watcher=OffsetWatcher(), key_formatter=KeyFormatter()
    ).initialize_karapace_producer()


def _rest_proxy_admin(config: Config) -> None:
    UserRestProxy(config, 1, MagicMock(spec=SchemaRegistrySerializer), verify_connection=False)


@pytest.mark.parametrize(
    "client_cls_path, create_client",
    [
        ("karapace.core.kafka_utils.KafkaAdminClient", _kafka_utils_admin),
        ("karapace.core.kafka_utils.KafkaConsumer", _kafka_utils_consumer),
        ("karapace.core.kafka_utils.KafkaProducer", _kafka_utils_producer),
        ("karapace.core.schema_reader.KafkaConsumer", _create_consumer_from_config),
        ("karapace.core.schema_reader.KafkaAdminClient", _create_admin_client_from_config),
        ("karapace.core.messaging.KafkaProducer", _karapace_producer),
        ("karapace.kafka_rest_apis.KafkaAdminClient", _rest_proxy_admin),
    ],
)
@pytest.mark.parametrize("debug", [None, DEBUG])
def test_sync_clients_get_librdkafka_debug(
    client_cls_path: str, create_client: Callable[[Config], None], debug: str | None
) -> None:
    with patch(client_cls_path) as client_cls:
        create_client(Config(librdkafka_debug=debug))

    assert client_cls.call_args.kwargs["debug"] == debug


@pytest.mark.parametrize("debug", [None, DEBUG])
async def test_rest_proxy_producer_gets_librdkafka_debug(debug: str | None) -> None:
    with patch("karapace.kafka_rest_apis.KafkaAdminClient"):
        proxy = UserRestProxy(Config(librdkafka_debug=debug), 1, MagicMock(spec=SchemaRegistrySerializer))

    with patch("karapace.kafka_rest_apis.AsyncKafkaProducer") as producer_cls:
        producer_cls.return_value.start = AsyncMock()
        await proxy._maybe_create_async_producer()

    assert producer_cls.call_args.kwargs["debug"] == debug


@pytest.mark.parametrize("debug", [None, DEBUG])
async def test_rest_proxy_consumer_gets_librdkafka_debug(debug: str | None) -> None:
    manager = ConsumerManager(Config(librdkafka_debug=debug), MagicMock(spec=SchemaRegistrySerializer))
    request_data = {"auto.offset.reset": "earliest", "auto.commit.enable": False, "consumer.request.timeout.ms": 1000}

    with patch("karapace.kafka_rest_apis.consumer_manager.AsyncKafkaConsumer") as consumer_cls:
        consumer_cls.return_value.start = AsyncMock()
        await manager.create_kafka_consumer(1, "group", "client", request_data)

    assert consumer_cls.call_args.kwargs["debug"] == debug
