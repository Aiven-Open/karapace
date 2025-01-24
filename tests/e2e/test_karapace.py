"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Iterator
from contextlib import closing, contextmanager, ExitStack
from pathlib import Path
from tests.integration.utils.kafka_server import KafkaServers
from tests.integration.utils.process import stop_process
from tests.utils import popen_karapace_all

import socket


@contextmanager
def allocate_port_no_reuse() -> Iterator[int]:
    """Allocate random free port and do not allow reuse."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        yield sock.getsockname()[1]


def test_regression_server_must_exit_on_exception(
    tmp_path: Path,
    kafka_servers: Iterator[KafkaServers],
) -> None:
    """Regression test for Karapace properly exiting.

    Karapace was not closing all its background threads, so when an exception
    was raised an reached the top-level, the webserver created by asyncio would
    be stopped but the threads would keep the server running.
    Karapace exit on exception is done by setting a reserved port as server port.
    """
    with ExitStack() as stack:
        karapace_rest_proxy_port = stack.enter_context(allocate_port_no_reuse())
        karapace_schema_registry_port = stack.enter_context(allocate_port_no_reuse())
        logfile = stack.enter_context((tmp_path / "karapace.log").open("w"))
        errfile = stack.enter_context((tmp_path / "karapace.err").open("w"))

        karapace_rest_proxy_env = {
            "KARAPACE_BOOTSTRAP_URI": kafka_servers.bootstrap_servers[0],
            "KARAPACE_PORT": str(karapace_rest_proxy_port),
            "KARAPACE_REGISTRY_HOST": "127.0.0.1",
            "KARAPACE_REGISTRY_PORT": str(karapace_schema_registry_port),
            "KARAPACE_KARAPACE_REST": "true",
        }
        karapace_rest_proxy = popen_karapace_all(
            module="karapace.core.karapace_all", env=karapace_rest_proxy_env, stdout=logfile, stderr=errfile
        )
        stack.callback(stop_process, karapace_rest_proxy)  # make sure to stop the process if the test fails
        assert karapace_rest_proxy.wait(timeout=10) != 0, "Process should have exited with an error, port is already is use"

        karapace_schema_registry_env = {
            "KARAPACE_BOOTSTRAP_URI": kafka_servers.bootstrap_servers[0],
            "KARAPACE_PORT": str(karapace_schema_registry_port),
            "KARAPACE_KARAPACE_REGISTRY": "true",
        }
        karapace_schema_registry = popen_karapace_all(
            module="schema_registry", env=karapace_schema_registry_env, stdout=logfile, stderr=errfile
        )
        stack.callback(stop_process, karapace_schema_registry)  # make sure to stop the process if the test fails
        assert (
            karapace_schema_registry.wait(timeout=10) != 0
        ), "Process should have exited with an error, port is already is use"
