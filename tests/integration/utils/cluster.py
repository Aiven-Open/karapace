"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import ExitStack, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from karapace.core.client import Client
from karapace.core.config import Config
from tests.integration.utils.network import allocate_port
from tests.integration.utils.process import stop_process, wait_for_port_subprocess
from tests.utils import new_random_name, popen_karapace_all, repeat_until_master_is_available


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
    with ExitStack() as stack:
        for pos, config in enumerate(config_templates):
            port = stack.enter_context(allocate_port())
            assert isinstance(port, int), "Port must be an integer"

            group_dir = data_dir / str(group_id)
            group_dir.mkdir(parents=True, exist_ok=True)
            log_path = group_dir / f"{pos}.log"
            error_path = group_dir / f"{pos}.error"

            logfile = stack.enter_context(open(log_path, "w"))
            errfile = stack.enter_context(open(error_path, "w"))

            env = {
                "KARAPACE_HOST": config.host,
                "KARAPACE_PORT": str(port),
                "KARAPACE_GROUP_ID": group_id,
                "KARAPACE_ADVERTISED_HOSTNAME": config.host,
                "KARAPACE_BOOTSTRAP_URI": config.bootstrap_uri,
                "KARAPACE_TOPIC_NAME": schemas_topic,
                "KARAPACE_LOG_LEVEL": "DEBUG",
                "KARAPACE_LOG_FORMAT": "%(asctime)s [%(threadName)s] %(filename)s:%(funcName)s:%(lineno)d %(message)s",
                "KARAPACE_KARAPACE_REGISTRY": "true",
                "KARAPACE_REGISTRY_AUTHFILE": config.registry_authfile if config.registry_authfile else "",
                "KARAPACE_SERVER_TLS_CERTFILE": config.server_tls_certfile if config.server_tls_certfile else "",
                "KARAPACE_SERVER_TLS_KEYFILE": config.server_tls_keyfile if config.server_tls_keyfile else "",
                "KARAPACE_USE_PROTOBUF_FORMATTER": "true" if config.use_protobuf_formatter else "false",
                "KARAPACE_WAITING_TIME_BEFORE_ACTING_AS_MASTER_MS": str(config.waiting_time_before_acting_as_master_ms),
                "KARAPACE_MASTER_ELIGIBILITY": str(config.master_eligibility),
            }
            process = popen_karapace_all(module="karapace", env=env, stdout=logfile, stderr=errfile)
            stack.callback(stop_process, process)
            all_processes.append((process, port, config.host))

            protocol = "http" if config.server_tls_keyfile is None else "https"
            endpoint = RegistryEndpoint(protocol, config.host, port)
            description = RegistryDescription(endpoint, schemas_topic)
            all_registries.append(description)

        for process, port, host in all_processes:
            wait_for_port_subprocess(port, process, hostname=host, wait_time=120)

        yield all_registries


@asynccontextmanager
async def after_master_is_available(
    registry_instances: list[RegistryDescription], server_ca: str | None
) -> AsyncIterator[None]:
    client = Client(
        server_uri=registry_instances[0].endpoint.to_url(),
        server_ca=server_ca,
    )
    try:
        await repeat_until_master_is_available(client)
        yield
    finally:
        await client.close()
