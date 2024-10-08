"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from collections.abc import Iterator
from contextlib import closing, contextmanager, ExitStack
from karapace.config import set_config_defaults
from pathlib import Path
from tests.integration.utils.kafka_server import KafkaServers
from tests.integration.utils.process import stop_process
from tests.utils import popen_karapace_all

import json
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
        port = stack.enter_context(allocate_port_no_reuse())

        config = set_config_defaults(
            {
                "bootstrap_uri": kafka_servers.bootstrap_servers,
                "karapace_registry": True,
                "port": port,
            }
        )
        config_path = tmp_path / "karapace.json"

        logfile = stack.enter_context((tmp_path / "karapace.log").open("w"))
        errfile = stack.enter_context((tmp_path / "karapace.err").open("w"))
        config_path.write_text(json.dumps(config))
        process = popen_karapace_all(config_path, logfile, errfile)
        stack.callback(stop_process, process)  # make sure to stop the process if the test fails
        assert process.wait(timeout=10) != 0, "Process should have exited with an error, port is already is use"
