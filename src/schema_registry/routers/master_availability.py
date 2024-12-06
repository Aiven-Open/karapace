"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from karapace.config import Config
from karapace.forward_client import ForwardClient
from karapace.schema_registry import KarapaceSchemaRegistry
from pydantic import BaseModel
from schema_registry.container import SchemaRegistryContainer
from typing import Final

import json
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
    are_we_master, master_url = await schema_registry.get_master()
    LOG.info("are master %s, master url %s", are_we_master, master_url)
    response.headers.update(NO_CACHE_HEADER)

    if (
        schema_registry.schema_reader.master_coordinator._sc is not None  # pylint: disable=protected-access
        and schema_registry.schema_reader.master_coordinator._sc.is_master_assigned_to_myself()  # pylint: disable=protected-access
    ):
        return MasterAvailabilityResponse(master_available=are_we_master)

    if master_url is None or f"{config.advertised_hostname}:{config.advertised_port}" in master_url:
        return NO_MASTER

    forward_response = await forward_client.forward_request_remote(request=request, primary_url=master_url)
    if isinstance(response, JSONResponse):
        response_json = json.loads(forward_response.body)
        return MasterAvailabilityResponse(master_available=response_json["master_availability"])

    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=forward_response.body,
    )
