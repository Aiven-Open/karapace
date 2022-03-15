"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from dataclasses import dataclass
from typing import List

import random
import socket


@dataclass(frozen=True)
class PortRangeInclusive:
    start: int
    end: int

    PRIVILEGE_END = 2 ** 10
    MAX_PORTS = 2 ** 16 - 1

    def __post_init__(self) -> None:
        # Make sure the range is valid and that we don't need to be root
        assert self.end > self.start, "there must be at least one port available"
        assert self.end <= self.MAX_PORTS, f"end must be lower than {self.MAX_PORTS}"
        assert self.start > self.PRIVILEGE_END, "start must not be a privileged port"

    def next_range(self, number_of_ports: int) -> "PortRangeInclusive":
        next_start = self.end + 1
        next_end = next_start + number_of_ports - 1  # -1 because the range is inclusive

        return PortRangeInclusive(next_start, next_end)


# To find a good port range use the following:
#
#   curl --silent 'https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.txt' | \
#       egrep -i -e '^\s*[0-9]+-[0-9]+\s*unassigned' | \
#       awk '{print $1}'
#
KAFKA_PORT_RANGE = PortRangeInclusive(48700, 48800)
ZK_PORT_RANGE = KAFKA_PORT_RANGE.next_range(100)
REGISTRY_PORT_RANGE = ZK_PORT_RANGE.next_range(100)
TESTS_PORT_RANGE = REGISTRY_PORT_RANGE.next_range(100)


def get_random_port(*, port_range: PortRangeInclusive, blacklist: List[int]) -> int:
    """Find a random port in the range `PortRangeInclusive`.

    Note:
        This function is *not* aware of the ports currently open in the system,
        the blacklist only prevents two services of the same type to randomly
        get the same ports for *a single test run*.

        Because of that, the port range should be chosen such that there is no
        system service in the range. Also note that running two sessions of the
        tests with the same range is not supported and will lead to flakiness.
    """
    assert port_range.start <= port_range.end, f"{port_range.start} must be less-than-or-equal to {port_range.end}"

    # +1 because randint is inclusive for both ends
    ports_in_range = (port_range.end - port_range.start) + 1
    assert len(blacklist) < ports_in_range, f"no free ports available. Range {port_range}, blacklist: {blacklist}"

    value = random.randint(port_range.start, port_range.end)
    while value in blacklist:
        value = random.randint(port_range.start, port_range.end)
    return value


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
