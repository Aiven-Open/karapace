from pathlib import Path

import json
import pytest

pytest_plugins = "aiohttp.pytest_plugin"


@pytest.fixture(scope="session", name="session_tmppath")
def fixture_session_tmppath(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("karapace")


@pytest.fixture(scope="session", name="default_config_path")
def fixture_default_config(session_tmppath: Path) -> str:
    path = session_tmppath / "karapace_config.json"
    path.write_text(json.dumps({"registry_host": "localhost", "registry_port": 8081}))
    return str(path)
