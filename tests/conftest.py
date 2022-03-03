from avro.compatibility import SchemaCompatibilityResult
from pathlib import Path
from typing import List, Optional

import pytest
import re
import ujson

pytest_plugins = "aiohttp.pytest_plugin"
KAFKA_BOOTSTRAP_SERVERS_OPT = "--kafka-bootstrap-servers"
KAFKA_VERION_OPT = "--kafka-version"
KAFKA_VERSION = "2.7.0"
LOG_DIR_OPT = "--log-dir"
VERSION_REGEX = "([0-9]+[.])*[0-9]+"


def pytest_assertrepr_compare(op, left, right) -> Optional[List[str]]:
    if isinstance(left, SchemaCompatibilityResult) and isinstance(right, SchemaCompatibilityResult) and op in ("==", "!="):
        lines = ["Comparing SchemaCompatibilityResult instances:"]

        def pad(depth: int, *msg: str) -> str:
            return "  " * depth + " ".join(msg)

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

            lines.append(pad(depth, "compatibility", str(obj.compatibility)))
            list_details("locations:", depth, list(obj.locations))
            list_details("messages:", depth, list(obj.messages))
            list_details("incompatibilities:", depth, [str(i) for i in obj.incompatibilities])

        depth = 1
        compatibility_details("Left:", depth, left)
        compatibility_details("Right:", depth, right)
        return lines

    return None


def split_by_comma(arg: str) -> List[str]:
    return arg.split(",")


def pytest_addoption(parser, pluginmanager) -> None:  # pylint: disable=unused-argument
    # Configuration options for the services started by the test suite
    parser.addoption(
        KAFKA_VERION_OPT,
        default=KAFKA_VERSION,
        help=f"Kafka version used by the test suite. (Incompatible with {KAFKA_BOOTSTRAP_SERVERS_OPT})",
    )
    parser.addoption(
        LOG_DIR_OPT,
        help=f"Directory to save Kafka/ZK logs (Incompatible with {KAFKA_BOOTSTRAP_SERVERS_OPT})",
    )

    # Configuration options for services external to the test suite
    parser.addoption(
        KAFKA_BOOTSTRAP_SERVERS_OPT,
        type=split_by_comma,
        help=(
            f"Kafka servers to be used for testing, format is comma separated "
            f"list of <server>:<port>. If provided the test suite will not start "
            f"a Kafka server. (Incompatible with {KAFKA_VERION_OPT})"
        ),
    )
    parser.addoption(
        "--registry-url",
        help=(
            "URL of a running Schema Registry instance. If provided the test "
            "suite will not start a Schema Registry instance"
        ),
    )
    parser.addoption(
        "--rest-url",
        help="URL of a running REST API instance. If provided the test suite will not start a REST API instance",
    )
    parser.addoption(
        "--server-ca",
        help="Certificate file used to validate the Schema Registry server.",
    )


@pytest.fixture(autouse=True, scope="session")
def fixture_validate_options(request) -> None:
    """This fixture only exists to validate the custom command line flags."""
    kafka_bootstrap_servers = request.config.getoption("kafka_bootstrap_servers")
    log_dir = request.config.getoption("log_dir")
    kafka_version = request.config.getoption("kafka_version")
    registry_url = request.config.getoption("registry_url")
    rest_url = request.config.getoption("rest_url")
    server_ca = request.config.getoption("server_ca")

    has_external_registry_or_rest = registry_url or rest_url

    if not re.match(VERSION_REGEX, kafka_version):
        msg = "Provided Kafka version has invalid format {kafka_version} should match {VERSION_REGEX}"
        raise ValueError(msg)

    if kafka_bootstrap_servers is not None and log_dir is not None:
        msg = f"{KAFKA_BOOTSTRAP_SERVERS_OPT} and {LOG_DIR_OPT} are incompatible options, only provide one of the two"
        raise ValueError(msg)

    if kafka_bootstrap_servers is not None and kafka_version is not None:
        msg = f"{KAFKA_BOOTSTRAP_SERVERS_OPT} and {KAFKA_VERION_OPT} are incompatible options, only provide one of the two"
        raise ValueError(msg)

    if server_ca and not has_external_registry_or_rest:
        msg = "When using a server CA, an external registry or rest URI must also be provided."
        raise ValueError(msg)

    if has_external_registry_or_rest and not kafka_bootstrap_servers:
        msg = "When using an external registry or rest, the kafka bootstrap URIs must also be provided."
        raise ValueError(msg)


@pytest.fixture(scope="session", name="session_datadir")
def fixture_session_datadir(tmp_path_factory) -> Path:
    """Data files generated throught the tests should be stored here.

    These files are NOT persisted.
    """
    return tmp_path_factory.mktemp("data")


@pytest.fixture(scope="session", name="session_logdir")
def fixture_session_logdir(request, tmp_path_factory, worker_id) -> Path:
    """All useful log data for debugging should be stored here.

    These files are persisted by the CI for debugging purposes.
    """
    log_dir = request.config.getoption("log_dir")

    if log_dir is None and worker_id == "master":
        path = tmp_path_factory.mktemp("log")
    elif log_dir is None:
        path = tmp_path_factory.getbasetemp().parent / "log"
        path.mkdir(parents=True, exist_ok=True)
    else:
        path = Path(log_dir)
        path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture(scope="session", name="default_config_path")
def fixture_default_config(session_logdir: Path) -> str:
    path = session_logdir / "karapace_config.json"
    path.write_text(ujson.dumps({"registry_host": "localhost", "registry_port": 8081}))
    return str(path)
