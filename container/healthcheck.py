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

timeout: Final = aiohttp.ClientTimeout(total=2)


async def check_ok(url: str) -> bool:
    async with aiohttp.ClientSession() as session:
        response = await session.get(url)
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
