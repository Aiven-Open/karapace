"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import APIRouter, Response
from karapace.config import LOG
from karapace.dependencies import ConfigDep, SchemaRegistryDep
from pydantic import BaseModel
from typing import Final

import logging

LOG = logging.getLogger(__name__)


class MasterAvailabilityResponse(BaseModel):
    master_available: bool


master_availability_router = APIRouter(
    prefix="/master_available",
    tags=["master_available"],
    responses={404: {"description": "Not found"}},
)

NO_MASTER: Final = MasterAvailabilityResponse(master_available=False)
NO_CACHE_HEADER: Final = {"Cache-Control": "no-store, no-cache, must-revalidate"}


@master_availability_router.get("")
async def master_available(
    config: ConfigDep,
    schema_registry: SchemaRegistryDep,
    response: Response,
) -> MasterAvailabilityResponse:
    primary_info = await self.schema_registry.get_master()
    self.log.info("are master %s, master url %s", primary_info.primary, primary_info.primary_url)
    response.headers.update(NO_CACHE_HEADER)

    if (
        schema_registry.schema_reader.master_coordinator._sc is not None  # pylint: disable=protected-access
        and schema_registry.schema_reader.master_coordinator._sc.is_master_assigned_to_myself()  # pylint: disable=protected-access
    ):
        return MasterAvailabilityResponse(master_available=are_we_master)

    if (
        primary_info.primary_url is None
        or f"{self.config['advertised_hostname']}:{self.config['advertised_port']}" in primary_info.primary_url
    ):
        return NO_MASTER

    # Forwarding not implemented yet.
    return NO_MASTER
