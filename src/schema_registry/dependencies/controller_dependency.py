"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""


from fastapi import Depends
from schema_registry.dependencies.config_dependency import ConfigDep
from schema_registry.dependencies.schema_registry_dependency import SchemaRegistryDep
from schema_registry.dependencies.stats_dependeny import StatsDep
from schema_registry.schema_registry_apis import KarapaceSchemaRegistryController
from typing import Annotated


async def get_controller(
    config: ConfigDep,
    stats: StatsDep,
    schema_registry: SchemaRegistryDep,
) -> KarapaceSchemaRegistryController:
    return KarapaceSchemaRegistryController(config=config, schema_registry=schema_registry, stats=stats)


KarapaceSchemaRegistryControllerDep = Annotated[KarapaceSchemaRegistryController, Depends(get_controller)]
