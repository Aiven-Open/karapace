"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from karapace.container import KarapaceContainer
from schema_registry.container import SchemaRegistryContainer
from schema_registry.factory import create_karapace_application, karapace_schema_registry_lifespan
from schema_registry.telemetry.container import TelemetryContainer

import karapace.coordinator.master_coordinator
import karapace.kafka.admin
import karapace.offset_watcher
import schema_registry.controller
import schema_registry.factory
import schema_registry.reader
import schema_registry.routers.compatibility
import schema_registry.routers.config
import schema_registry.routers.health
import schema_registry.routers.master_availability
import schema_registry.routers.metrics
import schema_registry.routers.mode
import schema_registry.routers.schemas
import schema_registry.routers.subjects
import schema_registry.telemetry.middleware
import schema_registry.telemetry.setup
import schema_registry.telemetry.tracer
import schema_registry.user
import uvicorn

if __name__ == "__main__":
    karapace_container = KarapaceContainer()
    karapace_container.wire(
        modules=[
            __name__,
            schema_registry.controller,
            schema_registry.telemetry.tracer,
        ]
    )

    telemetry_container = TelemetryContainer(karapace_container=karapace_container)
    telemetry_container.wire(
        modules=[
            schema_registry.telemetry.setup,
            schema_registry.telemetry.middleware,
            schema_registry.reader,
            karapace.offset_watcher,
            karapace.coordinator.master_coordinator,
            karapace.kafka.admin,
        ]
    )

    schema_registry_container = SchemaRegistryContainer(
        karapace_container=karapace_container, telemetry_container=telemetry_container
    )
    schema_registry_container.wire(
        modules=[
            __name__,
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

    config = karapace_container.config()
    app = create_karapace_application(config=config, lifespan=karapace_schema_registry_lifespan)
    uvicorn.run(app, host=config.host, port=config.port, log_level=config.log_level.lower())
