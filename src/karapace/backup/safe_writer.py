"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from tempfile import mkstemp, TemporaryDirectory
from typing import Final, IO, Literal, TypeAlias

import contextlib
import os
import sys

__all__ = ("OverwriteRefused", "bytes_writer", "str_writer")

StdOut: TypeAlias = Literal["", "-"]
std_out_alias: Final = ("", "-")


class OverwriteRefused(Exception):
    ...


def _check_destination_file(destination: Path, allow_overwrite: bool) -> None:
    if not destination.exists():
        return
    if not allow_overwrite:
        raise OverwriteRefused("Target file exists and overwriting is not enabled.")
    if not destination.is_file():
        raise OverwriteRefused("Target file is a directory, and cannot be overwritten.")


@contextlib.contextmanager
def _safe_temporary_descriptor(
    target: Path,
    allow_overwrite: bool,
) -> Generator[int, None, None]:
    """
    Helper function to take care of shared duties for safe file writing. Yields a file
    descriptor that can be passed to open() to acquire writable buffer. The file
    descriptor is a temporary file, that is transparently fsynced and then copied onto
    the target path when writing succeeds.

    Checks are made both as early as possible and as late as possibly to limit the risk
    of overwriting an existing file.

    :raises OverwriteRefused: either ``overwrite`` is not ``True`` and the file already
        exists, or the target file is a directory.
    """
    # Check destination does not already exist, in order to be able to abort as early as
    # possible if it does.
    _check_destination_file(target, allow_overwrite)

    # Create parent directory tree.
    target.parent.mkdir(parents=True, exist_ok=True)

    # Create temporary file. Note it's important it's next to target file, to make it
    # improbable that the temporary file and target path are on different volumes.
    fd, str_path = mkstemp(dir=target.parent, prefix=target.name)
    tmp_path = Path(str_path)

    try:
        yield fd

        # Check again that the target path hasn't been occupied while we were writing
        # to the temporary file.
        _check_destination_file(target, allow_overwrite)

        # Move temporary file to target path.
        tmp_path.replace(target)
    finally:
        # Always delete the temporary file.
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


@contextlib.contextmanager
def bytes_writer(
    target: Path | StdOut,
    allow_overwrite: bool,
) -> Generator[IO[bytes], None, None]:
    if not isinstance(target, Path):
        assert target in std_out_alias
        yield sys.stdout.buffer
        return

    safe_context = _safe_temporary_descriptor(target.absolute(), allow_overwrite)

    with safe_context as fd, open(fd, "wb") as buffer:
        yield buffer
        buffer.flush()
        os.fsync(fd)


@contextlib.contextmanager
def str_writer(
    target: Path | StdOut,
    allow_overwrite: bool,
) -> Generator[IO[str], None, None]:
    if not isinstance(target, Path):
        assert target in std_out_alias
        yield sys.stdout
        return

    safe_context = _safe_temporary_descriptor(target.absolute(), allow_overwrite)

    with safe_context as fd, open(fd, "w") as buffer:
        try:
            yield buffer
            buffer.flush()
            os.fsync(fd)
        finally:
            pass


def _check_destination_directory(destination: Path) -> None:
    if destination.exists():
        raise OverwriteRefused("Target directory unexpectedly exists.")


@contextlib.contextmanager
def staging_directory(target: Path) -> Generator[Path, None, None]:
    # Check destination does not already exist, in order to be able to abort as
    # early as possible if it does.
    _check_destination_directory(target)

    # Note it's important that temporary directory is next to target path, to make it
    # improbable that they are on different volumes.
    with TemporaryDirectory(dir=target.parent, prefix=target.name) as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        yield tmp_dir

        # Check again that the target path hasn't been occupied while we were
        # writing to the staging directory.
        _check_destination_directory(target)

        # Move staging directory to target path.
        tmp_dir.replace(target)
