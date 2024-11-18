"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import Depends
from karapace.config import Config
from karapace.forward_client import ForwardClient
from karapace.karapace_all import CONFIG, SCHEMA_REGISTRY
from karapace.schema_registry import KarapaceSchemaRegistry
from karapace.schema_registry_apis import KarapaceSchemaRegistryController
from karapace.statsd import StatsClient
from typing import Annotated


def get_config() -> Config:
    return CONFIG


ConfigDep = Annotated[Config, Depends(get_config)]


async def get_schema_registry() -> KarapaceSchemaRegistry:
    return SCHEMA_REGISTRY


SchemaRegistryDep = Annotated[KarapaceSchemaRegistry, Depends(get_schema_registry)]


async def get_stats(
    config: ConfigDep,
) -> StatsClient:
    return StatsClient(config=config)


StatsDep = Annotated[StatsClient, Depends(get_stats)]


async def get_controller(
    config: ConfigDep,
    stats: StatsDep,
    schema_registry: SchemaRegistryDep,
) -> KarapaceSchemaRegistryController:
    return KarapaceSchemaRegistryController(config=config, schema_registry=schema_registry, stats=stats)


KarapaceSchemaRegistryControllerDep = Annotated[KarapaceSchemaRegistryController, Depends(get_controller)]


FORWARD_CLIENT: ForwardClient | None = None


def get_forward_client() -> ForwardClient:
    global FORWARD_CLIENT
    if not FORWARD_CLIENT:
        FORWARD_CLIENT = ForwardClient()
    return FORWARD_CLIENT


ForwardClientDep = Annotated[ForwardClient, Depends(get_forward_client)]
