"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from karapace.utils import Expiration
from subprocess import Popen
from tests.integration.utils.network import port_is_listening

import os
import signal
import time


def wait_for_port_subprocess(
    port: int,
    process: Popen,
    *,
    hostname: str = "127.0.0.1",
    wait_time: float = 20.0,
    ipv6: bool = False,
) -> None:
    expiration = Expiration.from_timeout(wait_time)

    while not port_is_listening(hostname, port, ipv6):
        expiration.raise_timeout_if_expired(
            msg_format="Timeout waiting for `{hostname}:{port}`",
            hostname=hostname,
            port=port,
        )
        assert process.poll() is None, f"Process no longer running, exit_code: {process.returncode}"
        time.sleep(2.0)

    elapsed = expiration.elapsed
    print(f"Server `{hostname}:{port}` listening after {elapsed} seconds")


def stop_process(proc: Popen | None) -> None:
    if proc:
        try:
            os.kill(proc.pid, signal.SIGKILL)
            proc.wait(timeout=10.0)
        # ProcessLookupError: Raised when the process is already gone
        except ProcessLookupError:
            pass


def get_java_process_configuration(java_args: list[str]) -> list[str]:
    command = [
        "/usr/bin/java",
        "-server",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=20",
        "-XX:InitiatingHeapOccupancyPercent=35",
        "-XX:+DisableExplicitGC",
        "-XX:+ExitOnOutOfMemoryError",
        "-Djava.awt.headless=true",
        "-Dcom.sun.management.jmxremote",
        "-Dcom.sun.management.jmxremote.authenticate=false",
        "-Dcom.sun.management.jmxremote.ssl=false",
    ]
    command.extend(java_args)
    return command
