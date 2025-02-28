"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Final

import aiohttp
import asyncio
import sys
import ssl
import os

KARAPACE_ADVERTISED_PROTOCOL: Final = os.getenv("KARAPACE_ADVERTISED_PROTOCOL", "http")
KARAPACE_SERVER_TLS_CAFILE: Final = os.getenv("KARAPACE_SERVER_TLS_CAFILE")

timeout: Final = aiohttp.ClientTimeout(total=2)


async def check_ok(url: str) -> bool:
    ssl_context: ssl.SSLContext | None = None
    if KARAPACE_ADVERTISED_PROTOCOL == "https":
        ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.load_verify_locations(cafile=KARAPACE_SERVER_TLS_CAFILE)
    async with aiohttp.ClientSession() as session:
        response = await session.get(url, ssl=ssl_context)
        if response.status != HTTPStatus.OK:
            print(
                f"Server responded with non-OK {response.status=} {url=}",
                file=sys.stderr,
            )
            raise SystemExit(1)
        print("Ok!", file=sys.stderr)


def main() -> None:
    url = sys.argv[1]
    print(f"Checking {url=}", file=sys.stderr)
    asyncio.run(check_ok(url))


if __name__ == "__main__":
    main()
