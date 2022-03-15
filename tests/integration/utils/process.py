"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from subprocess import Popen
from tests.integration.utils.network import port_is_listening
from tests.utils import Expiration
from typing import List, Optional

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
    start_time = time.monotonic()
    expiration = Expiration(deadline=start_time + wait_time)
    msg = f"Timeout waiting for `{hostname}:{port}`"

    while not port_is_listening(hostname, port, ipv6):
        expiration.raise_if_expired(msg)
        assert process.poll() is None, f"Process no longer running, exit_code: {process.returncode}"
        time.sleep(2.0)

    elapsed = time.monotonic() - start_time
    print(f"Server `{hostname}:{port}` listening after {elapsed} seconds")


def stop_process(proc: Optional[Popen]) -> None:
    if proc:
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait(timeout=10.0)


def get_java_process_configuration(java_args: List[str]) -> List[str]:
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
