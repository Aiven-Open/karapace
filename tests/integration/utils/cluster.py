"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from contextlib import asynccontextmanager, ExitStack
from dataclasses import dataclass
from karapace.config import Config, set_config_defaults, write_config
from pathlib import Path
from subprocess import Popen
from tests.integration.utils.network import PortRangeInclusive
from tests.integration.utils.process import stop_process, wait_for_port_subprocess
from tests.utils import new_random_name
from typing import AsyncIterator, List


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
    config_templates: List[Config],
    data_dir: Path,
    port_range: PortRangeInclusive,
) -> AsyncIterator[List[RegistryDescription]]:
    """Start a cluster of schema registries, one process per `config_templates`."""
    for template in config_templates:
        assert "bootstrap_uri" in template, "base_config must have the value `bootstrap_uri` set"

    # None is considered a valid value, and it represents the lack of user
    # configuration, so this will generate one for the cluster
    group_ids = {config.get("group_id") for config in config_templates}
    assert len(group_ids) == 1, f"All configurations entries must have the same group_id value, got: {group_ids}"

    group_id = new_random_name("group_id")
    schemas_topic = new_random_name("_schemas")

    all_processes = []
    all_registries = []
    with ExitStack() as stack:
        for pos, template in enumerate(config_templates):
            config = dict(template)
            del template

            # For testing we don't want to expose the hostname, usually the loopback interface is
            # used (127.0.0.1), and the name resolution would instead return the machine's network
            # address, (e.g. 192.168.0.1), which would cause connect failures
            host = config.setdefault("host", "127.0.0.1")
            assert isinstance(host, str), "host must be str"
            config.setdefault("advertised_hostname", host)
            config.setdefault("topic_name", schemas_topic)
            config.setdefault("karapace_registry", True)
            config.setdefault(
                "log_format",
                "%(asctime)s [%(threadName)s] %(filename)s:%(funcName)s:%(lineno)d %(message)s",
            )
            actual_group_id = config.setdefault("group_id", group_id)

            port = config.setdefault("port", stack.enter_context(port_range.allocate_port()))
            assert isinstance(port, int), "Port must be an integer"

            group_dir = data_dir / str(actual_group_id)
            group_dir.mkdir(parents=True, exist_ok=True)
            config_path = group_dir / f"{pos}.config.json"
            log_path = group_dir / f"{pos}.log"
            error_path = group_dir / f"{pos}.error"

            config = set_config_defaults(config)
            write_config(config_path, config)

            logfile = stack.enter_context(open(log_path, "w"))
            errfile = stack.enter_context(open(error_path, "w"))
            process = Popen(
                args=["python", "-m", "karapace.karapace_all", str(config_path)],
                stdout=logfile,
                stderr=errfile,
            )
            stack.callback(stop_process, process)
            all_processes.append(process)

            protocol = "http" if config.get("server_tls_keyfile") is None else "https"
            endpoint = RegistryEndpoint(protocol, host, port)
            description = RegistryDescription(endpoint, schemas_topic)
            all_registries.append(description)

        for process in all_processes:
            wait_for_port_subprocess(port, process, hostname=host)

        yield all_registries
