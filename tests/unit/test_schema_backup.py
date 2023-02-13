"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.structs import PartitionMetadata
from karapace import config
from karapace.config import Config
from karapace.constants import DEFAULT_SCHEMA_TOPIC
from karapace.schema_backup import _admin, _consumer, _maybe_create_topic, _producer, _writer, PartitionCountError
from pathlib import Path
from types import FunctionType
from typing import AbstractSet, Callable, List, Type, Union
from unittest import mock
from unittest.mock import MagicMock

import pytest
import sys

ADMIN_NEW_FQN = f"{KafkaAdminClient.__module__}.{KafkaAdminClient.__qualname__}.__new__"


class TestAdmin:
    @mock.patch(ADMIN_NEW_FQN, autospec=True)
    def test_auto_closing(self, admin_new: MagicMock):
        admin_mock = admin_new.return_value
        with _admin(config.DEFAULTS) as admin:
            assert admin is admin_mock
        assert admin_mock.close.call_count == 1

    @mock.patch("time.sleep", autospec=True)
    @mock.patch(ADMIN_NEW_FQN, autospec=True)
    def test_retry(self, admin_new: MagicMock, sleep_mock: MagicMock) -> None:
        admin_mock = admin_new.return_value
        admin_new.side_effect = [Exception("1"), Exception("2"), admin_mock]
        with _admin(config.DEFAULTS) as admin:
            assert admin is admin_mock
        assert sleep_mock.call_count == 2  # proof that we waited between retries
        assert admin_mock.close.call_count == 1

    @pytest.mark.parametrize("e", (KeyboardInterrupt, SystemExit))
    @mock.patch("time.sleep", autospec=True)
    @mock.patch(ADMIN_NEW_FQN, autospec=True)
    def test_interrupts(self, admin_new: MagicMock, sleep_mock: MagicMock, e: Type[BaseException]) -> None:
        admin_new.side_effect = [e()]
        with pytest.raises(e):
            with _admin(config.DEFAULTS):
                pass
        assert sleep_mock.call_count == 0  # proof that we did not retry


class TestMaybeCreateTopic:
    @mock.patch(ADMIN_NEW_FQN, autospec=True)
    def test_ok(self, admin_new: MagicMock) -> None:
        assert _maybe_create_topic(config.DEFAULTS) is True
        create_topics: MagicMock = admin_new.return_value.create_topics
        assert create_topics.call_count == 1
        topic_list: List[NewTopic] = create_topics.call_args[0][0]
        assert len(topic_list) == 1
        assert topic_list[0].name == DEFAULT_SCHEMA_TOPIC

    @mock.patch(ADMIN_NEW_FQN, autospec=True)
    def test_exists(self, admin_new: MagicMock) -> None:
        create_topics: MagicMock = admin_new.return_value.create_topics
        create_topics.side_effect = TopicAlreadyExistsError()
        assert _maybe_create_topic(config.DEFAULTS) is False

    @mock.patch("time.sleep", autospec=True)
    @mock.patch(ADMIN_NEW_FQN, autospec=True)
    def test_retry(self, admin_new: MagicMock, sleep_mock: MagicMock) -> None:
        create_topics: MagicMock = admin_new.return_value.create_topics
        create_topics.side_effect = [Exception("1"), Exception("2"), None]
        assert _maybe_create_topic(config.DEFAULTS) is True
        assert sleep_mock.call_count == 2  # proof that we waited between retries

    def test_name_override(self) -> None:
        assert "custom-name" != DEFAULT_SCHEMA_TOPIC
        assert _maybe_create_topic(config.DEFAULTS, "custom-name") is None


class TestClients:
    @staticmethod
    def _partition_metadata(c: int = 1) -> AbstractSet[PartitionMetadata]:
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
        ctx_mng: Callable[[Config, str], Union[KafkaConsumer, KafkaProducer]],
        client_class: Type[Union[KafkaConsumer, KafkaProducer]],
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
    def test_partition_count_check(
        self,
        ctx_mng: Callable[[Config, str], Union[KafkaConsumer, KafkaProducer]],
        client_class: Type[Union[KafkaConsumer, KafkaProducer]],
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


class TestWrite:
    @pytest.mark.parametrize("it", ("", "-"))
    def test_stdout(self, it: str) -> None:
        with _writer(it) as fp:
            assert fp is sys.stdout

    def test_file_creation(self, tmp_path: Path) -> None:
        file = tmp_path / "file"
        with _writer(file) as fp:
            fp.write("test")
        assert list(tmp_path.iterdir()) == [file], "tmp file should be gone"
        assert file.read_text() == "test"

    def test_file_overwrite(self, tmp_path: Path) -> None:
        file = tmp_path / "file"
        file.touch()
        with _writer(file, overwrite=True) as fp:
            fp.write("test")
        assert list(tmp_path.iterdir()) == [file], "tmp file should be gone"
        assert file.read_text() == "test"

    def test_fails_if_file_exists_at_start(self, tmp_path: Path) -> None:
        file = tmp_path / "file"
        file.touch()
        with pytest.raises(FileExistsError):
            with _writer(file):
                pass
        assert list(tmp_path.iterdir()) == [file], "no tmp file is created"

    def test_fails_if_file_exists_at_end(self, tmp_path: Path) -> None:
        file = tmp_path / "file"
        with pytest.raises(FileExistsError):
            with _writer(file):
                file.touch()
        assert list(tmp_path.iterdir()) == [file], "tmp file should be gone"

    def test_fails_if_path_exists_and_is_not_a_file_despite_overwrite_flag_at_start(self, tmp_path: Path) -> None:
        file = tmp_path / "dir"
        file.mkdir()
        with pytest.raises(FileExistsError):
            with _writer(file, overwrite=True):
                pass
        assert list(tmp_path.iterdir()) == [file], "no tmp file is created"

    def test_fails_if_path_exists_and_is_not_a_file_despite_overwrite_flag_at_end(self, tmp_path: Path) -> None:
        file = tmp_path / "dir"
        with pytest.raises(FileExistsError):
            with _writer(file, overwrite=True):
                file.mkdir()
        assert list(tmp_path.iterdir()) == [file], "no tmp file is created"
