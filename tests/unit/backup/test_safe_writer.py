"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import secrets
import sys
from pathlib import Path

import pytest

from karapace.backup.safe_writer import (
    OverwriteRefused,
    StdOut,
    _check_destination_file,
    bytes_writer,
    staging_directory,
    str_writer,
)


class TestCheckDestinationFile:
    def test_returns_none_when_destination_does_not_exist(self) -> None:
        path = Path(secrets.token_hex(4))
        assert _check_destination_file(path, False) is None

    def test_returns_none_when_overwrite_is_allowed_and_destination_is_file(
        self,
        tmp_file: Path,
    ) -> None:
        assert _check_destination_file(tmp_file, True) is None

    def test_raises_overwrite_refused_when_not_allowed_and_destination_exists(
        self,
        tmp_file: Path,
    ) -> None:
        with pytest.raises(
            OverwriteRefused,
            match=r"^Target file exists and overwriting is not enabled.$",
        ):
            _check_destination_file(tmp_file, False)

    def test_raises_overwrite_refused_when_allowed_but_destination_is_directory(
        self,
        tmp_path: Path,
    ) -> None:
        with pytest.raises(
            OverwriteRefused,
            match=r"^Target file is a directory, and cannot be overwritten.$",
        ):
            _check_destination_file(tmp_path, True)


class TestStrWriter:
    @pytest.mark.parametrize("target", ("", "-"))
    def test_yields_std_out_buffer_for_std_out_alias(self, target: StdOut) -> None:
        with str_writer(target, False) as buffer:
            assert buffer is sys.stdout

    def test_can_create_file(self, tmp_path: Path) -> None:
        content = "test"
        target = tmp_path / "file"

        with str_writer(target, False) as buffer:
            buffer.write(content)

        assert target.read_text() == content
        assert list(tmp_path.iterdir()) == [target], "tmp file should be gone"

    def test_can_overwrite_file(self, tmp_path: Path) -> None:
        content = "test"
        target = tmp_path / "file"
        target.write_text("not test\nnot test")

        with str_writer(target, True) as buffer:
            buffer.write(content)

        assert target.read_text() == content
        assert list(tmp_path.iterdir()) == [target], "tmp file should be gone"

    def test_raises_overwrite_refused_if_file_exists_at_start(
        self,
        tmp_path: Path,
    ) -> None:
        target = tmp_path / "file"
        target.touch()

        context_reached = False
        raises_context = pytest.raises(
            OverwriteRefused,
            match=r"^Target file exists and overwriting is not enabled\.$",
        )

        with raises_context, str_writer(target, False):
            context_reached = True

        assert list(tmp_path.iterdir()) == [target], "no tmp file is created"
        assert not context_reached

    def test_raises_overwrite_refused_if_file_is_created_before_context_exit(
        self,
        tmp_path: Path,
    ) -> None:
        target = tmp_path / "file"

        raises_context = pytest.raises(
            OverwriteRefused,
            match=r"^Target file exists and overwriting is not enabled\.$",
        )

        with raises_context, str_writer(target, False):
            target.touch()

        assert list(tmp_path.iterdir()) == [target], "tmp file should be gone"

    def test_raises_overwrite_refused_for_directory_on_context_enter(
        self,
        tmp_path: Path,
    ) -> None:
        target = tmp_path / "dir"
        target.mkdir()

        context_reached = False
        raises_context = pytest.raises(
            OverwriteRefused,
            match=r"^Target file is a directory, and cannot be overwritten\.$",
        )

        with raises_context, str_writer(target, True):
            context_reached = True

        assert list(tmp_path.iterdir()) == [target], "no tmp file is created"
        assert not context_reached

    def test_raises_overwrite_refused_for_directory_on_context_exit(
        self,
        tmp_path: Path,
    ) -> None:
        target = tmp_path / "dir"

        raises_context = pytest.raises(
            OverwriteRefused,
            match=r"^Target file is a directory, and cannot be overwritten\.$",
        )

        with raises_context, str_writer(target, True):
            target.mkdir()

        assert list(tmp_path.iterdir()) == [target], "no tmp file is created"


class TestBytesWriter:
    @pytest.mark.parametrize("target", ("", "-"))
    def test_yields_std_out_buffer_for_std_out_alias(self, target: StdOut) -> None:
        with bytes_writer(target, False) as buffer:
            assert buffer is sys.stdout.buffer

    def test_can_create_file(self, tmp_path: Path) -> None:
        content = b"test"
        target = tmp_path / "file"

        with bytes_writer(target, False) as buffer:
            buffer.write(content)

        assert target.read_bytes() == content
        assert list(tmp_path.iterdir()) == [target], "tmp file should be gone"

    def test_can_overwrite_file(self, tmp_path: Path) -> None:
        content = b"test"
        target = tmp_path / "file"
        target.write_bytes(b"not test\nnot test")

        with bytes_writer(target, True) as buffer:
            buffer.write(content)

        assert target.read_bytes() == content
        assert list(tmp_path.iterdir()) == [target], "tmp file should be gone"

    def test_raises_overwrite_refused_if_file_exists_at_start(
        self,
        tmp_path: Path,
    ) -> None:
        target = tmp_path / "file"
        target.touch()

        context_reached = False
        raises_context = pytest.raises(
            OverwriteRefused,
            match=r"^Target file exists and overwriting is not enabled\.$",
        )

        with raises_context, bytes_writer(target, False):
            context_reached = True

        assert list(tmp_path.iterdir()) == [target], "no tmp file is created"
        assert not context_reached

    def test_raises_overwrite_refused_if_file_is_created_before_context_exit(
        self,
        tmp_path: Path,
    ) -> None:
        target = tmp_path / "file"

        raises_context = pytest.raises(
            OverwriteRefused,
            match=r"^Target file exists and overwriting is not enabled\.$",
        )

        with raises_context, bytes_writer(target, False):
            target.touch()

        assert list(tmp_path.iterdir()) == [target], "tmp file should be gone"

    def test_raises_overwrite_refused_for_directory_on_context_enter(
        self,
        tmp_path: Path,
    ) -> None:
        target = tmp_path / "dir"
        target.mkdir()

        context_reached = False
        raises_context = pytest.raises(
            OverwriteRefused,
            match=r"^Target file is a directory, and cannot be overwritten\.$",
        )

        with raises_context, bytes_writer(target, True):
            context_reached = True

        assert list(tmp_path.iterdir()) == [target], "no tmp file is created"
        assert not context_reached

    def test_raises_overwrite_refused_for_directory_on_context_exit(
        self,
        tmp_path: Path,
    ) -> None:
        target = tmp_path / "dir"

        raises_context = pytest.raises(
            OverwriteRefused,
            match=r"^Target file is a directory, and cannot be overwritten\.$",
        )

        with raises_context, bytes_writer(target, True):
            target.mkdir()

        assert list(tmp_path.iterdir()) == [target], "no tmp file is created"


class TestStagingDirectory:
    def test_can_create_directory(self, tmp_path: Path) -> None:
        target = tmp_path / "my-target"
        with staging_directory(target) as temporary:
            assert temporary.exists()
            assert not target.exists()
            assert temporary.parent == target.parent
            assert temporary.name.startswith(target.name)
            (temporary / "my-file").write_text("hello")

        # Only target exists in parent, i.e. the temporary directory is gone.
        assert list(target.parent.iterdir()) == [target]
        # Only the test file exists in the target directory.
        assert list(path.name for path in target.iterdir()) == ["my-file"]
        # The test file is intact.
        assert (target / "my-file").read_text() == "hello"

    def test_deletes_temporary_directory_on_exception(self, tmp_path: Path) -> None:
        target = tmp_path / "my-target"
        raises_context = pytest.raises(RuntimeError, match=r"^It broke!$")

        with raises_context, staging_directory(target) as temporary:
            assert temporary.exists()
            (temporary / "my-file").write_text("hello")
            raise RuntimeError("It broke!")

        assert list(target.parent.iterdir()) == []

    def test_raises_overwrite_refused_on_context_enter(self, tmp_path: Path) -> None:
        target = tmp_path / "dir"
        target.mkdir()

        context_reached = False
        raises_context = pytest.raises(
            OverwriteRefused,
            match=r"^Target directory unexpectedly exists\.$",
        )

        with raises_context, staging_directory(target):
            context_reached = True

        assert list(tmp_path.iterdir()) == [target], "no tmp file is created"
        assert not context_reached

    def test_raises_overwrite_refused_on_context_exit(self, tmp_path: Path) -> None:
        target = tmp_path / "dir"

        raises_context = pytest.raises(
            OverwriteRefused,
            match=r"^Target directory unexpectedly exists\.$",
        )

        with raises_context, staging_directory(target):
            target.mkdir()

        assert list(tmp_path.iterdir()) == [target], "no tmp file is created"
