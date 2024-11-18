"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import APIRouter

root_router = APIRouter(
    tags=["root"],
    responses={404: {"description": "Not found"}},
)


@root_router.get("/")
async def compatibility_post() -> dict:
    return {}
