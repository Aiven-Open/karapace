"""
karapace - version

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from typing import Final

import os
import pathlib
import subprocess

version_file: Final = pathlib.Path(__file__).parent.resolve() / "karapace/version.py"


def save_version(new_ver, old_ver):
    if not new_ver:
        return False
    if not old_ver or new_ver != old_ver:
        version_file.write_text(f'"""{__doc__}"""\n__version__ = "{new_ver}"\n')
    return True


def from_version_file() -> str | None:
    try:
        import karapace.version
    except ImportError:
        return None
    return karapace.version.__version__


def get_project_version() -> str:
    file_ver = from_version_file()

    version = os.getenv("KARAPACE_VERSION")
    if version is None:
        os.chdir(os.path.dirname(__file__) or ".")
        try:
            git_out = subprocess.check_output(
                ["git", "describe", "--always", "--tags"], stderr=getattr(subprocess, "DEVNULL", None)
            )
        except (OSError, subprocess.CalledProcessError):
            pass
        else:
            git_ver = git_out.splitlines()[0].strip().decode("utf-8")
            if "." not in git_ver:
                git_ver = f"0.0.1-0-unknown-{git_ver}"
            version = git_ver

    if save_version(version, file_ver):
        return version

    if not file_ver:
        raise RuntimeError(f"version not available from git or from file {str(version_file)!r}")

    return file_ver


if __name__ == "__main__":
    get_project_version()
