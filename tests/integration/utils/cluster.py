"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from karapace.config import Config
from pathlib import Path
from tests.utils import new_random_name


@dataclass(frozen=True)
class RegistryEndpoint:
    protocol: str
    host: str
    port: int

    def to_url(self) -> str:
        return f"{self.protocol}://{self.host}:{self.port}"


@dataclass(frozen=True)
class RegistryDescription:
    endpoint: RegistryEndpoint
    schemas_topic: str


@asynccontextmanager
async def start_schema_registry_cluster(
    config_templates: list[Config],
    data_dir: Path,
) -> AsyncIterator[list[RegistryDescription]]:
    """Start a cluster of schema registries, one process per `config_templates`."""
    # TODO clean
    # for template in config_templates:
    #    assert "bootstrap_uri" in template, "base_config must have the value `bootstrap_uri` set"

    # None is considered a valid value, and it represents the lack of user
    # configuration, so this will generate one for the cluster
    group_ids = {config.group_id for config in config_templates}
    assert len(group_ids) == 1, f"All configurations entries must have the same group_id value, got: {group_ids}"

    group_id = new_random_name("group_id")
    schemas_topic = new_random_name("_schemas")

    all_processes = []
    all_registries = []

    protocol = "http"
    endpoint = RegistryEndpoint(protocol, "karapace-schema-registry", 8081)
    description = RegistryDescription(endpoint, "_schemas")
    all_registries.append(description)
    yield all_registries
