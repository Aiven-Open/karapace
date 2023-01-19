"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
import socket


def inet_port() -> int:
    """Returns a free and usable IPv4 port.

    :raises OSError: if no free port can be found.
    """
    sock = socket.socket()
    sock.settimeout(0.1)
    try:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()
