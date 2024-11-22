"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import Depends
from karapace.forward_client import ForwardClient
from typing import Annotated

FORWARD_CLIENT: ForwardClient | None = None


def get_forward_client() -> ForwardClient:
    global FORWARD_CLIENT
    if not FORWARD_CLIENT:
        FORWARD_CLIENT = ForwardClient()
    return FORWARD_CLIENT


ForwardClientDep = Annotated[ForwardClient, Depends(get_forward_client)]
