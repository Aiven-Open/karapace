"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, Depends, Request, Response
from karapace.config import Config
from karapace.forward_client import ForwardClient
from karapace.schema_registry import KarapaceSchemaRegistry
from pydantic import BaseModel
from schema_registry.container import SchemaRegistryContainer
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
@inject
async def master_availability(
    request: Request,
    response: Response,
    config: Config = Depends(Provide[SchemaRegistryContainer.karapace_container.config]),
    forward_client: ForwardClient = Depends(Provide[SchemaRegistryContainer.karapace_container.forward_client]),
    schema_registry: KarapaceSchemaRegistry = Depends(Provide[SchemaRegistryContainer.karapace_container.schema_registry]),
) -> MasterAvailabilityResponse:
    primary_info = await schema_registry.get_master()
    LOG.info("are master %s, master url %s", primary_info.primary, primary_info.primary_url)
    response.headers.update(NO_CACHE_HEADER)

    if (
        schema_registry.schema_reader.master_coordinator is not None
        and schema_registry.schema_reader.master_coordinator._sc is not None  # pylint: disable=protected-access
        and schema_registry.schema_reader.master_coordinator._sc.is_master_assigned_to_myself()  # pylint: disable=protected-access
    ):
        return MasterAvailabilityResponse(master_available=primary_info.primary)

    if (
        primary_info.primary_url is None
        or f"{config.advertised_hostname}:{config.advertised_port}" in primary_info.primary_url
    ):
        return NO_MASTER

    return await forward_client.forward_request_remote(
        request=request, primary_url=primary_info.primary_url, response_type=MasterAvailabilityResponse
    )
