"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from karapace.api.container import SchemaRegistryContainer
from karapace.api.factory import create_karapace_application, karapace_schema_registry_lifespan
from karapace.api.telemetry.container import TelemetryContainer
from karapace.core.container import KarapaceContainer

import karapace.api.controller
import karapace.api.factory
import karapace.api.routers.compatibility
import karapace.api.routers.config
import karapace.api.routers.health
import karapace.api.routers.master_availability
import karapace.api.routers.metrics
import karapace.api.routers.mode
import karapace.api.routers.schemas
import karapace.api.routers.subjects
import karapace.api.telemetry.meter
import karapace.api.telemetry.middleware
import karapace.api.telemetry.setup
import karapace.api.telemetry.tracer
import karapace.api.user
import uvicorn

if __name__ == "__main__":
    karapace_container = KarapaceContainer()
    karapace_container.wire(
        modules=[
            __name__,
            karapace.api.controller,
            karapace.api.telemetry.tracer,
            karapace.api.telemetry.meter,
        ]
    )

    telemetry_container = TelemetryContainer(karapace_container=karapace_container)
    telemetry_container.wire(
        modules=[
            karapace.api.telemetry.setup,
            karapace.api.telemetry.middleware,
        ]
    )

    schema_registry_container = SchemaRegistryContainer(
        karapace_container=karapace_container, telemetry_container=telemetry_container
    )
    schema_registry_container.wire(
        modules=[
            __name__,
            karapace.api.factory,
            karapace.api.user,
            karapace.api.routers.health,
            karapace.api.routers.metrics,
            karapace.api.routers.subjects,
            karapace.api.routers.schemas,
            karapace.api.routers.config,
            karapace.api.routers.compatibility,
            karapace.api.routers.mode,
            karapace.api.routers.master_availability,
        ]
    )

    config = karapace_container.config()
    app = create_karapace_application(config=config, lifespan=karapace_schema_registry_lifespan)
    uvicorn.run(app, host=config.host, port=config.port, log_level=config.log_level.lower(), log_config=None)
