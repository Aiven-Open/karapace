"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import json
import os
import re
from pathlib import Path
from tempfile import mkstemp

import pytest
from avro.compatibility import SchemaCompatibilityResult

import karapace.api.controller
import karapace.core.instrumentation.meter
import karapace.api.telemetry.middleware
import karapace.api.telemetry.setup
import karapace.core.instrumentation.tracer
from karapace.api.container import SchemaRegistryContainer
from karapace.api.telemetry.container import TelemetryContainer
from karapace.core.auth_container import AuthContainer
from karapace.core.container import KarapaceContainer

pytest_plugins = "aiohttp.pytest_plugin"
KAFKA_BOOTSTRAP_SERVERS_OPT = "--kafka-bootstrap-servers"
KAFKA_VERION_OPT = "--kafka-version"
KAFKA_VERSION = "3.4.1"
LOG_DIR_OPT = "--log-dir"
VERSION_REGEX = "([0-9]+[.])*[0-9]+"


def pytest_assertrepr_compare(op, left, right) -> list[str] | None:
    if isinstance(left, SchemaCompatibilityResult) and isinstance(right, SchemaCompatibilityResult) and op in ("==", "!="):
        lines = ["Comparing SchemaCompatibilityResult instances:"]

        def pad(depth: int, *msg: str) -> str:
            return "  " * depth + " ".join(msg)

        def list_details(header: str, depth: int, items: list[str]) -> None:
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


def split_by_comma(arg: str) -> list[str]:
    return arg.split(",")


def pytest_addoption(parser, pluginmanager) -> None:
    # Configuration options for the services started by the test suite
    parser.addoption(
        KAFKA_VERION_OPT,
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
            "URL of a running Schema Registry instance. If provided the test suite will not start a Schema Registry instance"
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

    if kafka_version is not None and not re.match(VERSION_REGEX, kafka_version):
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
def fixture_default_config(session_logdir: Path) -> Path:
    path = session_logdir / "karapace_config.json"
    content = json.dumps({"registry_host": "localhost", "registry_port": 8081}).encode()
    content_len = len(content)
    with path.open("wb") as fp:
        written = fp.write(content)
        if written != content_len:
            raise OSError(f"Writing config failed, tried to write {content_len} bytes, but only {written} were written")
        fp.flush()
        os.fsync(fp)
    return path


@pytest.fixture(name="tmp_file", scope="function")
def fixture_tmp_file():
    _, str_path = mkstemp()
    path = Path(str_path)
    yield path
    path.unlink()


@pytest.fixture(name="karapace_container", scope="session", autouse=True)
def fixture_karapace_container() -> KarapaceContainer:
    karapace_container = KarapaceContainer()
    karapace_container.wire(
        modules=[
            karapace.core.instrumentation.tracer,
            karapace.core.instrumentation.meter,
        ]
    )
    return karapace_container


@pytest.fixture(name="telemetry_container", scope="session", autouse=True)
def fixture_telemetry_container() -> TelemetryContainer:
    telemetry_container = TelemetryContainer()
    telemetry_container.wire(
        modules=[
            karapace.api.telemetry.setup,
            karapace.api.telemetry.middleware,
        ]
    )
    return telemetry_container


@pytest.fixture(name="auth_container", scope="session", autouse=True)
def fixture_auth_container(karapace_container: KarapaceContainer) -> AuthContainer:
    auth_container = AuthContainer(karapace_container=karapace_container)
    auth_container.wire(modules=[karapace.api.controller])
    return auth_container


@pytest.fixture(name="schema_registry_container", scope="session", autouse=True)
def fixture_schema_registry_container(
    karapace_container: KarapaceContainer, telemetry_container: TelemetryContainer
) -> SchemaRegistryContainer:
    return SchemaRegistryContainer(karapace_container=karapace_container, telemetry_container=telemetry_container)
