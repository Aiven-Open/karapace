"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import logging
import os
import tarfile
import time
from dataclasses import dataclass
from pathlib import Path
from subprocess import Popen

import requests
from aiokafka.errors import AuthenticationFailedError, NoBrokersAvailable

from karapace.core.kafka.admin import KafkaAdminClient
from karapace.core.utils import Expiration
from tests.integration.utils.config import KafkaConfig, KafkaDescription, ZKConfig
from tests.integration.utils.process import get_java_process_configuration
from tests.utils import write_ini

log = logging.getLogger(__name__)


@dataclass
class KafkaServers:
    bootstrap_servers: list[str]

    def __post_init__(self) -> None:
        is_bootstrap_uris_valid = (
            isinstance(self.bootstrap_servers, list)
            and len(self.bootstrap_servers) > 0
            and all(isinstance(url, str) for url in self.bootstrap_servers)
        )
        if not is_bootstrap_uris_valid:
            raise ValueError("bootstrap_servers must be a non-empty list of urls")


def wait_for_kafka(
    kafka_servers: KafkaServers,
    wait_time: float,
) -> None:
    for server in kafka_servers.bootstrap_servers:
        expiration = Expiration.from_timeout(timeout=wait_time)

        list_topics_successful = False
        while not list_topics_successful:
            expiration.raise_timeout_if_expired(
                msg_format="Could not contact kafka cluster on host `{server}`",
                server=server,
            )
            try:
                KafkaAdminClient(bootstrap_servers=server).cluster_metadata()
            except (NoBrokersAvailable, AuthenticationFailedError) as e:
                print(f"Error checking kafka cluster: {e}")
                time.sleep(2.0)
            else:
                list_topics_successful = True


def maybe_download_kafka(kafka_description: KafkaDescription) -> None:
    """If necessary download kafka to run the tests."""
    if not os.path.exists(kafka_description.install_dir):
        log.info("Downloading Kafka '%s'", kafka_description.download_url)
        download = requests.get(kafka_description.download_url, stream=True)
        kafka_description.kafka_tgz.parent.mkdir(parents=True, exist_ok=True)
        with open(kafka_description.kafka_tgz, "wb") as fd:
            for chunk in download.iter_content(chunk_size=None):
                fd.write(chunk)

    if os.path.exists(kafka_description.install_dir):
        return

    log.info("Extracting Kafka '%s'", kafka_description.kafka_tgz)
    with open(kafka_description.kafka_tgz, "rb") as kafkatgz:
        with tarfile.open(mode="r:gz", fileobj=kafkatgz) as file:

            def is_within_directory(directory, target):
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)

                prefix = os.path.commonprefix([abs_directory, abs_target])

                return prefix == abs_directory

            def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                for member in tar.getmembers():
                    member_path = os.path.join(path, member.name)
                    if not is_within_directory(path, member_path):
                        raise RuntimeError("Attempted Path Traversal in Tar File")

                tar.extractall(path, members, numeric_owner=numeric_owner)

            safe_extract(file, str(kafka_description.install_dir.parent))


def kafka_java_args(
    heap_mb: int,
    kafka_config_path: str,
    logs_dir: str,
    log4j_properties_path: str,
    kafka_description: KafkaDescription,
) -> list[str]:
    msg = f"Couldn't find kafka installation at {kafka_description.install_dir} to run integration tests."
    assert kafka_description.install_dir.exists(), msg
    java_args = [
        f"-Xmx{heap_mb}M",
        f"-Xms{heap_mb}M",
        f"-Dkafka.logs.dir={logs_dir}/logs",
        f"-Dlog4j.configuration=file:{log4j_properties_path}",
        "-cp",
        str(kafka_description.install_dir / "libs" / "*"),
        "kafka.Kafka",
        kafka_config_path,
    ]
    return java_args


def configure_and_start_kafka(
    zk_config: ZKConfig,
    kafka_config: KafkaConfig,
    kafka_description: KafkaDescription,
    log4j_config: str,
    kafka_properties: dict[str, str | int],
) -> Popen:
    config_path = Path(kafka_config.logdir) / "server.properties"

    advertised_listeners = ",".join(
        [
            f"PLAINTEXT://127.0.0.1:{kafka_config.plaintext_port}",
        ]
    )
    listeners = ",".join(
        [
            f"PLAINTEXT://:{kafka_config.plaintext_port}",
        ]
    )

    # Keep in sync with containers/compose.yml
    kafka_ini = {
        "broker.id": 1,
        "broker.rack": "local",
        "advertised.listeners": advertised_listeners,
        "auto.create.topics.enable": False,
        "default.replication.factor": 1,
        "delete.topic.enable": "true",
        "inter.broker.listener.name": "PLAINTEXT",
        "inter.broker.protocol.version": kafka_description.protocol_version,
        "listeners": listeners,
        "log.cleaner.enable": "true",
        "log.dirs": kafka_config.datadir,
        "log.message.format.version": kafka_description.protocol_version,
        "log.retention.check.interval.ms": 300000,
        "log.segment.bytes": 200 * 1024 * 1024,  # 200 MiB
        "log.preallocate": False,
        "num.io.threads": 8,
        "num.network.threads": 112,
        "num.partitions": 1,
        "num.replica.fetchers": 4,
        "num.recovery.threads.per.data.dir": 1,
        "offsets.topic.replication.factor": 1,
        "socket.receive.buffer.bytes": 100 * 1024,
        "socket.request.max.bytes": 100 * 1024 * 1024,
        "socket.send.buffer.bytes": 100 * 1024,
        "transaction.state.log.min.isr": 1,
        "transaction.state.log.num.partitions": 16,
        "transaction.state.log.replication.factor": 1,
        "zookeeper.connection.timeout.ms": 6000,
        "zookeeper.connect": f"127.0.0.1:{zk_config.client_port}",
    }
    kafka_ini.update(kafka_properties)

    write_ini(config_path, kafka_ini)

    kafka_cmd = get_java_process_configuration(
        java_args=kafka_java_args(
            heap_mb=256,
            logs_dir=kafka_config.logdir,
            log4j_properties_path=log4j_config,
            kafka_config_path=str(config_path),
            kafka_description=kafka_description,
        ),
    )
    env: dict[bytes, bytes] = {}
    proc = Popen(kafka_cmd, env=env)
    return proc
