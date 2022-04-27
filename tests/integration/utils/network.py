"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from contextlib import contextmanager

import psutil
import socket


def is_time_wait(port: int) -> bool:
    """True if the port is still on TIME_WAIT state."""
    return any(conn.laddr.port == port for conn in psutil.net_connections(kind="inet"))


class PortRangeInclusive:
    PRIVILEGE_END = 2**10
    MAX_PORTS = 2**16 - 1

    def __init__(
        self,
        start: int,
        end: int,
    ) -> None:
        # Make sure the range is valid and that we don't need to be root
        assert end > start, "there must be at least one port available"
        assert end <= self.MAX_PORTS, f"end must be lower than {self.MAX_PORTS}"
        assert start > self.PRIVILEGE_END, "start must not be a privileged port"

        self.start = start
        self.end = end
        self._maybe_available = list(range(start, end + 1))

    def next_range(self, number_of_ports: int) -> "PortRangeInclusive":
        next_start = self.end
        next_end = next_start + number_of_ports
        return PortRangeInclusive(next_start, next_end)

    @contextmanager
    def allocate_port(self) -> int:
        """Find a random port in the range `PortRangeInclusive`.

        Note:
            This function is *not* aware of the ports currently open in the system,
            the blacklist only prevents two services of the same type to randomly
            get the same ports for *a single test run*.

            Because of that, the port range should be chosen such that there is no
            system service in the range. Also note that running two sessions of the
            tests with the same range is not supported and will lead to flakiness.
        """
        if len(self._maybe_available) == 0:
            raise RuntimeError(f"No free ports available. start: {self.start} end: {self.end}")

        filtered_ports = ((pos, port) for pos, port in enumerate(self._maybe_available) if not is_time_wait(port))

        try:
            pos, port = next(filtered_ports)
        except StopIteration as e:
            raise RuntimeError(
                f"No free ports available. start: {self.start} end: {self.end} time_wait: {self._maybe_available}"
            ) from e

        self._maybe_available.pop(pos)
        yield port

        # Append the port at the end, this is a hack to give extra time for a TIME_WAIT socket to
        # close, but it is not sufficient.
        self._maybe_available.append(port)


def port_is_listening(hostname: str, port: int, ipv6: bool) -> bool:
    if ipv6:
        s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM, 0)
    else:
        s = socket.socket()
    s.settimeout(0.5)
    try:
        s.connect((hostname, port))
        s.close()
        return True
    except socket.error:
        return False
