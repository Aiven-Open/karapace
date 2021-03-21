from karapace.avro_compatibility import SchemaCompatibilityResult
from pathlib import Path
from typing import List, Optional

import json
import pytest

pytest_plugins = "aiohttp.pytest_plugin"


def pytest_assertrepr_compare(op, left, right) -> Optional[List[str]]:
    if isinstance(left, SchemaCompatibilityResult) and isinstance(right, SchemaCompatibilityResult) and op in ("==", "!="):
        lines = ["Comparing SchemaCompatibilityResult instances:"]

        def pad(depth: int, *msg: str) -> str:
            return "  " * depth + ' '.join(msg)

        def list_details(header: str, depth: int, items: List[str]) -> None:
            qty = len(items)

            if qty == 1:
                lines.append(pad(depth, header, *items))
            elif qty > 1:
                lines.append(pad(depth, header))
                depth += 1
                for loc in items:
                    lines.append(pad(depth, loc))

        def compatibility_details(header: str, depth: int, obj: SchemaCompatibilityResult) -> None:
            lines.append(pad(depth, header))

            depth += 1

            lines.append(pad(depth, 'compatibility', str(obj.compatibility)))
            list_details('locations:', depth, list(obj.locations))
            list_details('messages:', depth, list(obj.messages))
            list_details('incompatibilities:', depth, [str(i) for i in obj.incompatibilities])

        depth = 1
        compatibility_details("Left:", depth, left)
        compatibility_details("Right:", depth, right)
        return lines

    return None


def split_by_comma(arg: str) -> List[str]:
    return arg.split(',')


def pytest_addoption(parser, pluginmanager) -> None:  # pylint: disable=unused-argument
    parser.addoption('--kafka-bootstrap-servers', type=split_by_comma)


@pytest.fixture(scope="session", name="session_tmppath")
def fixture_session_tmppath(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("karapace")


@pytest.fixture(scope="session", name="default_config_path")
def fixture_default_config(session_tmppath: Path) -> str:
    path = session_tmppath / "karapace_config.json"
    path.write_text(json.dumps({"registry_host": "localhost", "registry_port": 8081}))
    return str(path)
