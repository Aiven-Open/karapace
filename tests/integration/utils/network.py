"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from contextlib import closing, contextmanager
from typing import Iterator

import socket


@contextmanager
def allocate_port() -> Iterator[int]:
    """Allocate random free port."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        yield sock.getsockname()[1]


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
    except OSError:
        return False
