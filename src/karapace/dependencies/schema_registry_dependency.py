"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import Depends
from karapace.dependencies.config_dependency import ConfigDependencyManager
from karapace.schema_registry import KarapaceSchemaRegistry
from typing import Annotated


class SchemaRegistryDependencyManager:
    SCHEMA_REGISTRY: KarapaceSchemaRegistry | None = None

    @classmethod
    async def get_schema_registry(cls) -> KarapaceSchemaRegistry:
        if not SchemaRegistryDependencyManager.SCHEMA_REGISTRY:
            SchemaRegistryDependencyManager.SCHEMA_REGISTRY = KarapaceSchemaRegistry(
                config=ConfigDependencyManager.get_config()
            )
        return SchemaRegistryDependencyManager.SCHEMA_REGISTRY


SchemaRegistryDep = Annotated[KarapaceSchemaRegistry, Depends(SchemaRegistryDependencyManager.get_schema_registry)]
