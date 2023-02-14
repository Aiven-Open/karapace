"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import PartitionMetadata
from karapace import config
from karapace.config import Config
from karapace.schema_backup import _consumer, _producer, _writer, PartitionCountError
from pathlib import Path
from types import FunctionType
from typing import AbstractSet, Callable, Type, Union
from unittest import mock

import pytest
import sys


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
