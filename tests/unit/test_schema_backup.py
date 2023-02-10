"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from karapace.schema_backup import _writer
from pathlib import Path

import pytest
import sys


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
