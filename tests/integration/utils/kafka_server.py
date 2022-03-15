"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from kafka.errors import LeaderNotAvailableError, NoBrokersAvailable
from karapace.kafka_rest_apis import KafkaRestAdminClient
from karapace.utils import Expiration
from pathlib import Path
from subprocess import Popen
from tests.integration.utils.config import KafkaConfig, KafkaDescription, ZKConfig
from tests.integration.utils.process import get_java_process_configuration
from tests.utils import get_random_port, KAFKA_PORT_RANGE, KafkaServers, write_ini
from typing import Dict, List, Tuple

import logging
import os
import requests
import tarfile
import time


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
                KafkaRestAdminClient(bootstrap_servers=server).cluster_metadata()
            # ValueError:
            # - if the port number is invalid (i.e. not a number)
            # - if the port is not bound yet
            # NoBrokersAvailable:
            # - if the address/port does not point to a running server
            # LeaderNotAvailableError:
            # - if there is no leader yet
            except (
                NoBrokersAvailable,
                LeaderNotAvailableError,
                ValueError,
            ) as e:
                print(f"Error checking kafka cluster: {e}")
                time.sleep(2.0)
            else:
                list_topics_successful = True


def maybe_download_kafka(kafka_description: KafkaDescription) -> None:
    """If necessary download kafka to run the tests."""
    if not os.path.exists(kafka_description.install_dir):
        logging.info("Downloading Kafka {url}", url=kafka_description.download_url)

        download = requests.get(kafka_description.download_url, stream=True)
        with tarfile.open(mode="r:gz", fileobj=download.raw) as file:
            file.extractall(str(kafka_description.install_dir.parent))


def kafka_java_args(
    heap_mb: int,
    kafka_config_path: str,
    logs_dir: str,
    log4j_properties_path: str,
    kafka_description: KafkaDescription,
) -> List[str]:
    msg = f"Couldn't find kafka installation at {kafka_description.install_dir} to run integration tests."
    assert kafka_description.install_dir.exists(), msg
    java_args = [
        "-Xmx{}M".format(heap_mb),
        "-Xms{}M".format(heap_mb),
        "-Dkafka.logs.dir={}/logs".format(logs_dir),
        "-Dlog4j.configuration=file:{}".format(log4j_properties_path),
        "-cp",
        str(kafka_description.install_dir / "libs" / "*"),
        "kafka.Kafka",
        kafka_config_path,
    ]
    return java_args


def configure_and_start_kafka(
    datadir: Path,
    logdir: Path,
    log4j_config: Path,
    zk: ZKConfig,
    kafka_description: KafkaDescription,
) -> Tuple[KafkaConfig, Popen]:
    # setup filesystem
    data_dir = datadir / "kafka"
    log_dir = logdir / "kafka"
    config_path = log_dir / "server.properties"
    data_dir.mkdir(parents=True)
    log_dir.mkdir(parents=True)

    plaintext_port = get_random_port(port_range=KAFKA_PORT_RANGE, blacklist=[])

    config = KafkaConfig(
        datadir=str(data_dir),
        kafka_keystore_password="secret",
        kafka_port=plaintext_port,
        zookeeper_port=zk.client_port,
    )

    advertised_listeners = ",".join(
        [
            "PLAINTEXT://127.0.0.1:{}".format(plaintext_port),
        ]
    )
    listeners = ",".join(
        [
            "PLAINTEXT://:{}".format(plaintext_port),
        ]
    )

    # Keep in sync with containers/docker-compose.yml
    kafka_config = {
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
        "log.dirs": config.datadir,
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
        "zookeeper.connect": f"127.0.0.1:{zk.client_port}",
    }

    write_ini(config_path, kafka_config)

    # stdout logger is disabled to keep the pytest report readable
    log4j_properties_path = str(log4j_config)

    kafka_cmd = get_java_process_configuration(
        java_args=kafka_java_args(
            heap_mb=256,
            logs_dir=str(log_dir),
            log4j_properties_path=log4j_properties_path,
            kafka_config_path=str(config_path),
            kafka_description=kafka_description,
        ),
    )
    env: Dict[bytes, bytes] = {}
    proc = Popen(kafka_cmd, env=env)
    return config, proc
