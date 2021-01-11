from tests.utils import TempDirCreator
from typing import Iterable

import contextlib
import json
import os
import pytest

pytest_plugins = "aiohttp.pytest_plugin"


@pytest.fixture(scope="session", name="session_tmpdir")
def fixture_session_tmpdir(tmpdir_factory) -> Iterable[TempDirCreator]:
    """Create a temporary directory object that's usable in the session scope.  The returned value is a
    function which creates a new temporary directory which will be automatically cleaned up upon exit."""
    tmpdir_obj = tmpdir_factory.mktemp("karapace.sssion.tmpdr.")

    def subdir():
        return tmpdir_obj.mkdtemp(rootdir=tmpdir_obj)

    try:
        yield subdir
    finally:
        with contextlib.suppress(Exception):
            tmpdir_obj.remove(rec=1)


@pytest.fixture(scope="session", name="default_config_path")
def fixture_default_config(session_tmpdir: TempDirCreator) -> str:
    base_name = "karapace_config.json"
    path = os.path.join(session_tmpdir(), base_name)
    with open(path, 'w') as cf:
        cf.write(json.dumps({"registry_host": "localhost", "registry_port": 8081}))
    return path
