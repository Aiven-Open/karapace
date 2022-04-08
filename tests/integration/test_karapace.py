from contextlib import ExitStack
from karapace.config import set_config_defaults
from pathlib import Path
from subprocess import Popen
from tests.integration.utils.network import PortRangeInclusive
from tests.integration.utils.process import stop_process

import socket
import ujson


def test_regression_server_must_exit_on_exception(
    port_range: PortRangeInclusive,
    tmp_path: Path,
) -> None:
    """Regression test for Karapace properly exiting.

    Karapace was not closing all its background threads, so when an exception
    was raised an reached the top-level, the webserver created by asyncio would
    be stopped but the threads would keep the server running.
    """
    with ExitStack() as stack:
        port = stack.enter_context(port_range.allocate_port())
        sock = stack.enter_context(socket.socket())

        config = set_config_defaults(
            {
                "karapace_registry": True,
                "port": port,
            }
        )
        config_path = tmp_path / "karapace.json"

        logfile = stack.enter_context((tmp_path / "karapace.log").open("w"))
        errfile = stack.enter_context((tmp_path / "karapace.err").open("w"))
        config_path.write_text(ujson.dumps(config))
        sock.bind(("127.0.0.1", port))
        process = Popen(
            args=["python", "-m", "karapace.karapace_all", str(config_path)],
            stdout=logfile,
            stderr=errfile,
        )
        stack.callback(stop_process, process)  # make sure to stop the process if the test fails
        assert process.wait(timeout=10) != 0, "Process should have exited with an error, port is already is use"
