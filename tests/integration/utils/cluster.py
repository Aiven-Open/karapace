"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, ExitStack
from dataclasses import dataclass
from karapace.config import Config, write_env_file
from pathlib import Path
from tests.integration.utils.network import allocate_port
from tests.integration.utils.process import stop_process, wait_for_port_subprocess
from tests.utils import new_random_name, popen_karapace_all


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
            # For testing we don't want to expose the hostname, usually the loopback interface is
            # used (127.0.0.1), and the name resolution would instead return the machine's network
            # address, (e.g. 192.168.0.1), which would cause connect failures
            host = config.host
            config.advertised_hostname = host
            config.topic_name = schemas_topic
            config.karapace_registry = True
            config.log_level = "DEBUG"
            config.log_format = "%(asctime)s [%(threadName)s] %(filename)s:%(funcName)s:%(lineno)d %(message)s"
            actual_group_id = config.group_id = group_id

            port = config.port = stack.enter_context(allocate_port())
            assert isinstance(port, int), "Port must be an integer"

            group_dir = data_dir / str(actual_group_id)
            group_dir.mkdir(parents=True, exist_ok=True)
            env_path = group_dir / f"{pos}.env"
            log_path = group_dir / f"{pos}.log"
            error_path = group_dir / f"{pos}.error"

            # config = set_config_defaults(config)
            write_env_file(env_path, config)

            logfile = stack.enter_context(open(log_path, "w"))
            errfile = stack.enter_context(open(error_path, "w"))
            process = popen_karapace_all(host=host, port=port, env_path=env_path, stdout=logfile, stderr=errfile)
            stack.callback(stop_process, process)
            all_processes.append(process)

            protocol = "http" if config.server_tls_keyfile is None else "https"
            endpoint = RegistryEndpoint(protocol, host, port)
            description = RegistryDescription(endpoint, schemas_topic)
            all_registries.append(description)

        for process in all_processes:
            wait_for_port_subprocess(port, process, hostname=host)

        yield all_registries
