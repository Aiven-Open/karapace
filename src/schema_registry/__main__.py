"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from karapace.container import KarapaceContainer
from schema_registry.container import SchemaRegistryContainer
from schema_registry.factory import create_karapace_application, karapace_schema_registry_lifespan

import schema_registry.factory
import schema_registry.middlewares.prometheus
import schema_registry.routers.compatibility
import schema_registry.routers.config
import schema_registry.routers.health
import schema_registry.routers.master_availability
import schema_registry.routers.metrics
import schema_registry.routers.mode
import schema_registry.routers.schemas
import schema_registry.routers.subjects
import schema_registry.schema_registry_apis
import schema_registry.user
import uvicorn

if __name__ == "__main__":
    container = KarapaceContainer()
    container.wire(
        modules=[
            __name__,
            schema_registry.schema_registry_apis,
        ]
    )

    schema_registry_container = SchemaRegistryContainer(karapace_container=container)
    schema_registry_container.wire(
        modules=[
            __name__,
            schema_registry.middlewares.prometheus,
            schema_registry.factory,
            schema_registry.user,
            schema_registry.routers.health,
            schema_registry.routers.metrics,
            schema_registry.routers.subjects,
            schema_registry.routers.schemas,
            schema_registry.routers.config,
            schema_registry.routers.compatibility,
            schema_registry.routers.mode,
            schema_registry.routers.master_availability,
        ]
    )

    app = create_karapace_application(config=container.config(), lifespan=karapace_schema_registry_lifespan)
    uvicorn.run(
        app, host=container.config().host, port=container.config().port, log_level=container.config().log_level.lower()
    )
