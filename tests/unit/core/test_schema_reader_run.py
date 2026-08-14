"""
Tests for `KafkaSchemaReader.run()` -- the thread's bootstrap and main consume loop.

The real method retries Kafka client/topic creation with real sleeps
(``KAFKA_CLIENT_CREATION_TIMEOUT_SECONDS`` / ``SCHEMA_TOPIC_CREATION_TIMEOUT_SECONDS``).
Tests here stub out ``_stop_schema_reader.wait`` so retries are instantaneous, and
drive every branch by scripting the Kafka client factories / admin client / consumer
mocks to raise, retry, and finally flip ``_stop_schema_reader`` so `run()` returns.

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from unittest.mock import ANY, Mock, patch

import pytest
from aiokafka.errors import (
    InvalidReplicationFactorError,
    KafkaConfigurationError,
    KafkaUnavailableError,
    NoBrokersAvailable,
    NodeNotReadyError,
    TopicAlreadyExistsError,
)
from confluent_kafka import TopicPartition

from karapace.core.container import KarapaceContainer
from karapace.core.errors import ShutdownException
from karapace.core.in_memory_database import InMemoryDatabase
from karapace.core.key_format import KeyFormatter
from karapace.core.offset_watcher import OffsetWatcher
from karapace.core.schema_reader import KafkaSchemaReader, _create_admin_client_from_config, _create_consumer_from_config
from karapace.core.stats import StatsClient


def _make_schema_reader(karapace_container: KarapaceContainer) -> KafkaSchemaReader:
    reader = KafkaSchemaReader(
        config=karapace_container.config(),
        offset_watcher=OffsetWatcher(),
        key_formatter=Mock(spec=KeyFormatter),
        master_coordinator=None,
        database=InMemoryDatabase(),
        stats=Mock(spec=StatsClient),
    )
    # Retry backoffs must not actually sleep in tests.
    reader._stop_schema_reader.wait = Mock(return_value=False)
    return reader


def _new_topic_stopping_side_effect(reader: KafkaSchemaReader):
    """Flip the stop flag and report "already exists" so the topic-creation loop
    exits normally and the (now-stopped) main consume loop is skipped entirely."""

    def _side_effect(*_args, **_kwargs):
        reader._stop_schema_reader.set()
        raise TopicAlreadyExistsError()

    return _side_effect


class TestRunAdminClientCreation:
    def test_retries_on_no_brokers_available_then_succeeds(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        admin_client_mock = Mock()
        admin_client_mock.new_topic.side_effect = _new_topic_stopping_side_effect(reader)
        consumer_mock = Mock()
        consumer_mock.assignment.return_value = []

        with (
            patch(
                "karapace.core.schema_reader._create_admin_client_from_config",
                side_effect=[NoBrokersAvailable(), admin_client_mock],
            ) as mock_create_admin,
            patch("karapace.core.schema_reader._create_consumer_from_config", return_value=consumer_mock),
        ):
            reader.run()

        assert mock_create_admin.call_count == 2
        assert reader.admin_client is admin_client_mock

    def test_reports_unexpected_exception_to_stats_and_retries(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        admin_client_mock = Mock()
        admin_client_mock.new_topic.side_effect = _new_topic_stopping_side_effect(reader)
        consumer_mock = Mock()
        consumer_mock.assignment.return_value = []

        with (
            patch(
                "karapace.core.schema_reader._create_admin_client_from_config",
                side_effect=[RuntimeError("boom"), admin_client_mock],
            ),
            patch("karapace.core.schema_reader._create_consumer_from_config", return_value=consumer_mock),
        ):
            reader.run()

        reader.stats.unexpected_exception.assert_any_call(ex=ANY, where="admin_client_instantiation")

    def test_configuration_error_propagates_and_sets_stop(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)

        with (
            patch(
                "karapace.core.schema_reader._create_admin_client_from_config",
                side_effect=KafkaConfigurationError("bad config"),
            ),
            pytest.raises(KafkaConfigurationError),
        ):
            reader.run()

        assert reader._stop_schema_reader.is_set()


class TestRunConsumerCreation:
    def test_retries_on_node_not_ready_then_succeeds(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        admin_client_mock = Mock()
        admin_client_mock.new_topic.side_effect = _new_topic_stopping_side_effect(reader)
        consumer_mock = Mock()
        consumer_mock.assignment.return_value = []

        with (
            patch("karapace.core.schema_reader._create_admin_client_from_config", return_value=admin_client_mock),
            patch(
                "karapace.core.schema_reader._create_consumer_from_config",
                side_effect=[NodeNotReadyError(), consumer_mock],
            ) as mock_create_consumer,
        ):
            reader.run()

        assert mock_create_consumer.call_count == 2
        assert reader.consumer is consumer_mock

    def test_reports_unexpected_exception_to_stats_and_retries(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        admin_client_mock = Mock()
        admin_client_mock.new_topic.side_effect = _new_topic_stopping_side_effect(reader)
        consumer_mock = Mock()
        consumer_mock.assignment.return_value = []

        with (
            patch("karapace.core.schema_reader._create_admin_client_from_config", return_value=admin_client_mock),
            patch(
                "karapace.core.schema_reader._create_consumer_from_config",
                side_effect=[RuntimeError("boom"), consumer_mock],
            ),
        ):
            reader.run()

        reader.stats.unexpected_exception.assert_any_call(ex=ANY, where="consumer_instantiation")

    def test_configuration_error_propagates_and_sets_stop(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)

        with (
            patch("karapace.core.schema_reader._create_admin_client_from_config", return_value=Mock()),
            patch(
                "karapace.core.schema_reader._create_consumer_from_config",
                side_effect=KafkaConfigurationError("bad config"),
            ),
            pytest.raises(KafkaConfigurationError),
        ):
            reader.run()

        assert reader._stop_schema_reader.is_set()


class TestRunTopicCreation:
    def test_retries_on_invalid_replication_factor_then_succeeds(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        admin_client_mock = Mock()
        call_count = 0

        def _new_topic_side_effect(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise InvalidReplicationFactorError()
            reader._stop_schema_reader.set()
            raise TopicAlreadyExistsError()

        admin_client_mock.new_topic.side_effect = _new_topic_side_effect
        consumer_mock = Mock()
        consumer_mock.assignment.return_value = []

        with (
            patch("karapace.core.schema_reader._create_admin_client_from_config", return_value=admin_client_mock),
            patch("karapace.core.schema_reader._create_consumer_from_config", return_value=consumer_mock),
        ):
            reader.run()

        assert call_count == 2

    def test_retries_on_unexpected_exception_then_succeeds(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        admin_client_mock = Mock()
        call_count = 0

        def _new_topic_side_effect(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("boom")
            reader._stop_schema_reader.set()
            raise TopicAlreadyExistsError()

        admin_client_mock.new_topic.side_effect = _new_topic_side_effect
        consumer_mock = Mock()
        consumer_mock.assignment.return_value = []

        with (
            patch("karapace.core.schema_reader._create_admin_client_from_config", return_value=admin_client_mock),
            patch("karapace.core.schema_reader._create_consumer_from_config", return_value=consumer_mock),
        ):
            reader.run()

        assert call_count == 2

    def test_already_exists_is_treated_as_success(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        admin_client_mock = Mock()
        admin_client_mock.new_topic.side_effect = _new_topic_stopping_side_effect(reader)
        consumer_mock = Mock()
        consumer_mock.assignment.return_value = []

        with (
            patch("karapace.core.schema_reader._create_admin_client_from_config", return_value=admin_client_mock),
            patch("karapace.core.schema_reader._create_consumer_from_config", return_value=consumer_mock),
        ):
            reader.run()  # must not raise

    def test_never_assigned_partitions_logs_warning_and_continues(self, karapace_container: KarapaceContainer) -> None:
        """`consumer.assignment()` staying empty for all 10 attempts must hit the
        `for ... else` warning branch, not raise."""
        reader = _make_schema_reader(karapace_container)
        admin_client_mock = Mock()
        admin_client_mock.new_topic.side_effect = _new_topic_stopping_side_effect(reader)
        consumer_mock = Mock()
        consumer_mock.assignment.return_value = []  # never becomes truthy

        with (
            patch("karapace.core.schema_reader._create_admin_client_from_config", return_value=admin_client_mock),
            patch("karapace.core.schema_reader._create_consumer_from_config", return_value=consumer_mock),
        ):
            reader.run()

        assert consumer_mock.poll.call_count == 10

    def test_assignment_succeeding_early_breaks_the_wait_loop(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        admin_client_mock = Mock()
        admin_client_mock.new_topic.side_effect = _new_topic_stopping_side_effect(reader)
        consumer_mock = Mock()
        consumer_mock.assignment.side_effect = [[], [TopicPartition("test-topic", 0)]]

        with (
            patch("karapace.core.schema_reader._create_admin_client_from_config", return_value=admin_client_mock),
            patch("karapace.core.schema_reader._create_consumer_from_config", return_value=consumer_mock),
        ):
            reader.run()

        assert consumer_mock.poll.call_count == 1


class TestRunMainLoop:
    def _reader_ready_to_enter_main_loop(self, karapace_container: KarapaceContainer) -> KafkaSchemaReader:
        reader = _make_schema_reader(karapace_container)
        admin_client_mock = Mock()
        admin_client_mock.new_topic.return_value = Mock(topic="test-topic")
        consumer_mock = Mock()
        consumer_mock.assignment.return_value = [TopicPartition("test-topic", 0)]
        consumer_mock.get_watermark_offsets.return_value = (0, 0)
        reader._admin_client_patch = patch(
            "karapace.core.schema_reader._create_admin_client_from_config", return_value=admin_client_mock
        )
        reader._consumer_patch = patch(
            "karapace.core.schema_reader._create_consumer_from_config", return_value=consumer_mock
        )
        return reader

    def test_processes_one_iteration_then_stops_cleanly(self, karapace_container: KarapaceContainer) -> None:
        reader = self._reader_ready_to_enter_main_loop(karapace_container)

        def _stop_after_one(*_args, **_kwargs):
            reader._stop_schema_reader.set()

        with (
            reader._admin_client_patch,
            reader._consumer_patch,
            patch.object(reader, "handle_messages", side_effect=_stop_after_one) as mock_handle,
        ):
            reader.run()

        mock_handle.assert_called_once()
        assert reader.consecutive_unexpected_errors == 0

    def test_shutdown_exception_stops_reader_and_calls_shutdown(self, karapace_container: KarapaceContainer) -> None:
        reader = self._reader_ready_to_enter_main_loop(karapace_container)

        with (
            reader._admin_client_patch,
            reader._consumer_patch,
            patch.object(reader, "handle_messages", side_effect=ShutdownException()),
            patch("karapace.core.schema_reader.shutdown") as mock_shutdown,
        ):
            reader.run()

        mock_shutdown.assert_called_once_with()
        assert reader._stop_schema_reader.is_set()

    def test_kafka_unavailable_error_increments_error_counter(self, karapace_container: KarapaceContainer) -> None:
        reader = self._reader_ready_to_enter_main_loop(karapace_container)

        def _side_effect():
            reader._stop_schema_reader.set()
            raise KafkaUnavailableError("unavailable")

        with (
            reader._admin_client_patch,
            reader._consumer_patch,
            patch.object(reader, "handle_messages", side_effect=_side_effect),
        ):
            reader.run()

        assert reader.consecutive_unexpected_errors == 1

    def test_unexpected_exception_reports_stats_and_tracks_error_window(self, karapace_container: KarapaceContainer) -> None:
        reader = self._reader_ready_to_enter_main_loop(karapace_container)

        def _side_effect():
            reader._stop_schema_reader.set()
            raise RuntimeError("boom")

        with (
            reader._admin_client_patch,
            reader._consumer_patch,
            patch.object(reader, "handle_messages", side_effect=_side_effect),
        ):
            reader.run()

        reader.stats.unexpected_exception.assert_called_once_with(ex=ANY, where="schema_reader_loop")
        assert reader.consecutive_unexpected_errors == 1
        assert reader.consecutive_unexpected_errors_start > 0


class TestClientFactories:
    """Covers the module-level `_create_consumer_from_config` / `_create_admin_client_from_config`
    helpers, including the branch where an OAuth token provider is (or isn't) configured."""

    def test_create_consumer_from_config_without_token_provider(self, karapace_container: KarapaceContainer) -> None:
        config = karapace_container.config()
        with (
            patch("karapace.core.schema_reader.KafkaConsumer") as consumer_cls,
            patch("karapace.core.schema_reader.get_oauth_token_provider", return_value=None) as get_token,
        ):
            result = _create_consumer_from_config(config)

        get_token.assert_called_once_with(config)
        consumer_cls.assert_called_once()
        kwargs = consumer_cls.call_args.kwargs
        assert "sasl_oauth_token_provider" not in kwargs
        assert kwargs["bootstrap_servers"] == config.bootstrap_uri
        assert kwargs["enable_auto_commit"] is False
        assert result is consumer_cls.return_value

    def test_create_consumer_from_config_with_token_provider(self, karapace_container: KarapaceContainer) -> None:
        config = karapace_container.config()
        token_provider = Mock()
        with (
            patch("karapace.core.schema_reader.KafkaConsumer") as consumer_cls,
            patch("karapace.core.schema_reader.get_oauth_token_provider", return_value=token_provider),
        ):
            _create_consumer_from_config(config)

        assert consumer_cls.call_args.kwargs["sasl_oauth_token_provider"] is token_provider

    def test_create_admin_client_from_config_without_token_provider(self, karapace_container: KarapaceContainer) -> None:
        config = karapace_container.config()
        with (
            patch("karapace.core.schema_reader.KafkaAdminClient") as admin_cls,
            patch("karapace.core.schema_reader.get_oauth_token_provider", return_value=None) as get_token,
        ):
            result = _create_admin_client_from_config(config)

        get_token.assert_called_once_with(config)
        kwargs = admin_cls.call_args.kwargs
        assert "sasl_oauth_token_provider" not in kwargs
        assert kwargs["bootstrap_servers"] == config.bootstrap_uri
        assert result is admin_cls.return_value

    def test_create_admin_client_from_config_with_token_provider(self, karapace_container: KarapaceContainer) -> None:
        config = karapace_container.config()
        token_provider = Mock()
        with (
            patch("karapace.core.schema_reader.KafkaAdminClient") as admin_cls,
            patch("karapace.core.schema_reader.get_oauth_token_provider", return_value=token_provider),
        ):
            _create_admin_client_from_config(config)

        assert admin_cls.call_args.kwargs["sasl_oauth_token_provider"] is token_provider
