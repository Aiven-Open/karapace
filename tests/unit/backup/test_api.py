"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError
from kafka.structs import PartitionMetadata
from karapace import config
from karapace.backup.api import (
    _admin,
    _consumer,
    _handle_restore_topic,
    _handle_restore_topic_legacy,
    _maybe_create_topic,
    _producer,
    locate_backup_file,
    normalize_location,
    normalize_topic_name,
)
from karapace.backup.backends.reader import RestoreTopic, RestoreTopicLegacy
from karapace.backup.backends.writer import StdOut
from karapace.backup.errors import BackupError, PartitionCountError
from karapace.config import Config
from karapace.constants import DEFAULT_SCHEMA_TOPIC, TOPIC_CREATION_TIMEOUT_MS
from pathlib import Path
from types import FunctionType
from typing import Callable, cast, ContextManager
from unittest import mock
from unittest.mock import MagicMock

import pytest

patch_admin_new = mock.patch(
    "karapace.backup.api.KafkaAdminClient.__new__",
    autospec=True,
)


class TestAdmin:
    @patch_admin_new
    def test_auto_closing(self, admin_new: MagicMock) -> None:
        admin_mock = admin_new.return_value
        with _admin(config.DEFAULTS) as admin:
            assert admin is admin_mock
        assert admin_mock.close.call_count == 1

    @mock.patch("time.sleep", autospec=True)
    @patch_admin_new
    def test_retries_on_kafka_error(self, admin_new: MagicMock, sleep_mock: MagicMock) -> None:
        admin_mock = admin_new.return_value
        admin_new.side_effect = [KafkaError("1"), KafkaError("2"), admin_mock]
        with _admin(config.DEFAULTS) as admin:
            assert admin is admin_mock
        assert sleep_mock.call_count == 2  # proof that we waited between retries
        assert admin_mock.close.call_count == 1

    @pytest.mark.parametrize("e", (KeyboardInterrupt, SystemExit, RuntimeError, MemoryError))
    @mock.patch("time.sleep", autospec=True)
    @patch_admin_new
    def test_reraises_unknown_exceptions(
        self,
        admin_new: MagicMock,
        sleep_mock: MagicMock,
        e: type[BaseException],
    ) -> None:
        admin_new.side_effect = e
        with pytest.raises(e), _admin(config.DEFAULTS):
            pass
        assert sleep_mock.call_count == 0  # proof that we did not retry


class TestHandleRestoreTopic:
    @patch_admin_new
    def test_calls_admin_create_topics(self, admin_new: MagicMock) -> None:
        create_topics: MagicMock = admin_new.return_value.create_topics
        topic_configs = {"cleanup.policy": "compact"}
        _maybe_create_topic(DEFAULT_SCHEMA_TOPIC, config=config.DEFAULTS, replication_factor=1, topic_configs=topic_configs)

        create_topics.assert_called_once_with(mock.ANY, timeout_ms=TOPIC_CREATION_TIMEOUT_MS)
        ((new_topic,),) = create_topics.call_args.args
        assert isinstance(new_topic, NewTopic)
        assert new_topic.name == DEFAULT_SCHEMA_TOPIC
        assert new_topic.num_partitions == 1
        assert new_topic.replication_factor == config.DEFAULTS["replication_factor"]
        assert new_topic.topic_configs == topic_configs

    @patch_admin_new
    def test_gracefully_handles_topic_already_exists_error(self, admin_new: MagicMock) -> None:
        create_topics: MagicMock = admin_new.return_value.create_topics
        create_topics.side_effect = TopicAlreadyExistsError()
        _maybe_create_topic(DEFAULT_SCHEMA_TOPIC, config=config.DEFAULTS, replication_factor=1, topic_configs={})
        create_topics.assert_called_once()

    @patch_admin_new
    def test_retries_for_kafka_errors(self, admin_new: MagicMock) -> None:
        create_topics: MagicMock = admin_new.return_value.create_topics
        create_topics.side_effect = [KafkaError("1"), KafkaError("2"), None]

        with mock.patch("time.sleep", autospec=True):
            _maybe_create_topic(DEFAULT_SCHEMA_TOPIC, config=config.DEFAULTS, replication_factor=1, topic_configs={})

        assert create_topics.call_count == 3

    @patch_admin_new
    def test_noop_for_custom_name_on_legacy_versions(
        self,
        admin_new: MagicMock,
    ) -> None:
        create_topics: MagicMock = admin_new.return_value.create_topics
        assert "custom-name" != DEFAULT_SCHEMA_TOPIC
        instruction = RestoreTopicLegacy(topic_name="custom-name", partition_count=1)
        _handle_restore_topic_legacy(instruction, config.DEFAULTS)
        create_topics.assert_not_called()

    @patch_admin_new
    def test_allows_custom_name_on_v3(
        self,
        admin_new: MagicMock,
    ) -> None:
        create_topics: MagicMock = admin_new.return_value.create_topics
        topic_name = "custom-name"
        assert topic_name != DEFAULT_SCHEMA_TOPIC
        topic_configs = {"segment.bytes": "1000"}
        instruction = RestoreTopic(
            topic_name="custom-name", partition_count=1, replication_factor=2, topic_configs=topic_configs
        )
        _handle_restore_topic(instruction, config.DEFAULTS)

        create_topics.assert_called_once_with(mock.ANY, timeout_ms=TOPIC_CREATION_TIMEOUT_MS)
        ((new_topic,),) = create_topics.call_args.args
        assert isinstance(new_topic, NewTopic)
        assert new_topic.name == topic_name
        assert new_topic.num_partitions == 1
        assert new_topic.replication_factor == 2
        assert new_topic.topic_configs == topic_configs


class TestClients:
    @staticmethod
    def _partition_metadata(c: int = 1) -> set[PartitionMetadata]:
        return {PartitionMetadata("topic", i, 0, tuple(), tuple(), None) for i in range(0, c)}

    @pytest.mark.parametrize(
        "ctx_mng,client_class,partitions_method",
        (
            (_consumer, KafkaConsumer, KafkaConsumer.partitions_for_topic),
            (_producer, KafkaProducer, KafkaProducer.partitions_for),
        ),
    )
    def test_auto_closing(
        self,
        ctx_mng: Callable[[Config, str], ContextManager[KafkaConsumer | KafkaProducer]],
        client_class: type[KafkaConsumer | KafkaProducer],
        partitions_method: FunctionType,
    ) -> None:
        with mock.patch(f"{client_class.__module__}.{client_class.__qualname__}.__new__", autospec=True) as client_ctor:
            client_mock = client_ctor.return_value
            getattr(client_mock, partitions_method.__name__).return_value = self._partition_metadata()
            with ctx_mng(config.DEFAULTS, "topic") as client:
                assert client is client_mock
            assert client_mock.close.call_count == 1

    @pytest.mark.parametrize("partition_count", (0, 2))
    @pytest.mark.parametrize(
        "ctx_mng,client_class,partitions_method",
        (
            (_consumer, KafkaConsumer, KafkaConsumer.partitions_for_topic),
            (_producer, KafkaProducer, KafkaProducer.partitions_for),
        ),
    )
    def test_raises_partition_count_error_for_unexpected_count(
        self,
        ctx_mng: Callable[[Config, str], KafkaConsumer | KafkaProducer],
        client_class: type[KafkaConsumer | KafkaProducer],
        partitions_method: FunctionType,
        partition_count: int,
    ) -> None:
        with mock.patch(f"{client_class.__module__}.{client_class.__qualname__}.__new__", autospec=True) as client_ctor:
            client_mock = client_ctor.return_value
            getattr(client_mock, partitions_method.__name__).return_value = self._partition_metadata(partition_count)
            with pytest.raises(PartitionCountError):
                with ctx_mng(config.DEFAULTS, "topic") as client:
                    assert client == client_mock
            assert client_mock.close.call_count == 1


class TestNormalizeLocation:
    @pytest.mark.parametrize("alias", ("", "-"))
    def test_returns_dash_for_stdout_alias(self, alias: str) -> None:
        assert normalize_location(alias) == "-"

    def test_returns_absolute_path(self) -> None:
        normalized = normalize_location("some/file/path")
        assert isinstance(normalized, Path)
        assert normalized.is_absolute()


class TestLocateBackupFile:
    @pytest.mark.parametrize("alias", ("", "-"))
    def test_raises_backup_error_for_stdout_alias(self, alias: StdOut) -> None:
        with pytest.raises(BackupError, match=r"^Cannot restore backups from stdin$"):
            locate_backup_file(alias)

    def test_raises_backup_error_for_empty_directory(self, tmp_path: Path) -> None:
        with pytest.raises(
            BackupError,
            match=r"^When a given location is a directory, it must contain exactly one metadata file, found 0\.$",
        ):
            locate_backup_file(tmp_path)

    def test_raises_backup_error_for_multiple_metadata_files(self, tmp_path: Path) -> None:
        (tmp_path / "foo.metadata").touch()
        (tmp_path / "bar.metadata").touch()
        with pytest.raises(
            BackupError,
            match=r"^When a given location is a directory, it must contain exactly one metadata file, found 2\.$",
        ):
            locate_backup_file(tmp_path)

    def test_raises_backup_error_for_non_existent_path(self, tmp_path: Path) -> None:
        path = tmp_path / "foo"
        with pytest.raises(
            BackupError,
            match=r"^Backup location doesn't exist$",
        ):
            locate_backup_file(path)

    def test_raises_backup_error_for_non_file(self, tmp_path: Path) -> None:
        (tmp_path / "foo.metadata").mkdir()
        with pytest.raises(
            BackupError,
            match=r"^The normalized path is not a file$",
        ):
            locate_backup_file(tmp_path)

    def test_returns_path_to_nested_metadata(self, tmp_path: Path) -> None:
        metadata_path = tmp_path / "foo.metadata"
        metadata_path.touch()
        normalized = locate_backup_file(tmp_path)
        assert normalized == metadata_path

    def test_returns_given_file_path(self, tmp_path: Path) -> None:
        path = tmp_path / "foo"
        path.touch()
        normalized = locate_backup_file(path)
        assert normalized == path


class TestNormalizeTopicName:
    def test_returns_option_if_given(self) -> None:
        fake_config = cast(Config, {})
        assert normalize_topic_name("some-topic", fake_config) == "some-topic"

    def test_defaults_to_config(self) -> None:
        fake_config = cast(Config, {"topic_name": "default-topic"})
        assert normalize_topic_name(None, fake_config) == "default-topic"
