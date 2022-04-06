"""
karapace - conftest

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from _pytest.fixtures import SubRequest
from aiohttp import ClientSession
from aiohttp.pytest_plugin import AiohttpClient
from contextlib import closing, ExitStack
from dataclasses import asdict
from filelock import FileLock
from kafka import KafkaProducer
from karapace.client import Client
from karapace.config import set_config_defaults, write_config
from karapace.kafka_rest_apis import KafkaRest, KafkaRestAdminClient
from karapace.schema_registry_apis import KarapaceSchemaRegistry
from pathlib import Path
from subprocess import Popen
from tests.integration.utils.config import KafkaConfig, KafkaDescription, ZKConfig
from tests.integration.utils.kafka_server import (
    configure_and_start_kafka,
    KafkaServers,
    maybe_download_kafka,
    wait_for_kafka,
)
from tests.integration.utils.network import get_random_port, KAFKA_PORT_RANGE, REGISTRY_PORT_RANGE, ZK_PORT_RANGE
from tests.integration.utils.process import stop_process, wait_for_port_subprocess
from tests.integration.utils.synchronization import lock_path_for
from tests.integration.utils.zookeeper import configure_and_start_zk
from tests.utils import new_random_name, repeat_until_successful_request
from typing import AsyncIterator, Iterator, Optional, Tuple

import asyncio
import os
import pathlib
import pytest
import time
import ujson

REPOSITORY_DIR = pathlib.Path(__file__).parent.parent.parent.absolute()
RUNTIME_DIR = REPOSITORY_DIR / "runtime"
TEST_INTEGRATION_DIR = REPOSITORY_DIR / "tests" / "integration"
KAFKA_WAIT_TIMEOUT = 60
KAFKA_SCALA_VERSION = "2.13"
# stdout logger is disabled to keep the pytest report readable
KAFKA_LOG4J = TEST_INTEGRATION_DIR / "config" / "log4j.properties"
WORKER_COUNTER_KEY = "worker_counter"


@pytest.fixture(scope="session", name="kafka_description")
def fixture_kafka_description(request: SubRequest) -> KafkaDescription:
    kafka_version = request.config.getoption("kafka_version")
    kafka_folder = f"kafka_{KAFKA_SCALA_VERSION}-{kafka_version}"
    kafka_tgz = f"{kafka_folder}.tgz"
    kafka_url = f"https://archive.apache.org/dist/kafka/{kafka_version}/{kafka_tgz}"
    kafka_dir = RUNTIME_DIR / kafka_folder

    return KafkaDescription(
        version=kafka_version,
        install_dir=kafka_dir,
        download_url=kafka_url,
        protocol_version="2.7",
    )


@pytest.fixture(scope="session", name="kafka_servers")
def fixture_kafka_server(
    request: SubRequest,
    session_datadir: Path,
    session_logdir: Path,
    kafka_description: KafkaDescription,
) -> Iterator[KafkaServers]:
    bootstrap_servers = request.config.getoption("kafka_bootstrap_servers")

    if bootstrap_servers:
        kafka_servers = KafkaServers(bootstrap_servers)
        wait_for_kafka(kafka_servers, KAFKA_WAIT_TIMEOUT)
        yield kafka_servers
        return

    zk_dir = session_logdir / "zk"

    # File used to share data among test runners, including the dynamic
    # configuration for this session (mainly port numbers), and and
    # synchronization data.
    transfer_file = session_logdir / "transfer"
    lock_file = lock_path_for(transfer_file)

    with ExitStack() as stack:
        with FileLock(str(lock_file)):
            if transfer_file.exists():
                config_data = ujson.loads(transfer_file.read_text())
                zk_config = ZKConfig.from_dict(config_data["zookeeper"])
                kafka_config = KafkaConfig.from_dict(config_data["kafka"])

                # Count the new worker
                config_data[WORKER_COUNTER_KEY] += 1
                transfer_file.write_text(ujson.dumps(config_data))
            else:
                maybe_download_kafka(kafka_description)

                zk_client_port = get_random_port(port_range=ZK_PORT_RANGE, blacklist=[])
                zk_admin_port = get_random_port(port_range=ZK_PORT_RANGE, blacklist=[zk_client_port])
                zk_config = ZKConfig(
                    client_port=zk_client_port,
                    admin_port=zk_admin_port,
                    path=str(zk_dir),
                )

                zk_proc = configure_and_start_zk(zk_config, kafka_description)
                stack.callback(stop_process, zk_proc)

                # Make sure zookeeper is running before trying to start Kafka
                wait_for_port_subprocess(zk_config.client_port, zk_proc, wait_time=20)

                kafka_plaintext_port = get_random_port(port_range=KAFKA_PORT_RANGE, blacklist=[])
                data_dir = session_datadir / "kafka"
                log_dir = session_logdir / "kafka"
                data_dir.mkdir(parents=True)
                log_dir.mkdir(parents=True)
                kafka_config = KafkaConfig(
                    datadir=str(data_dir),
                    logdir=str(log_dir),
                    plaintext_port=kafka_plaintext_port,
                )
                kafka_proc = configure_and_start_kafka(
                    zk_config=zk_config,
                    kafka_config=kafka_config,
                    kafka_description=kafka_description,
                    log4j_config=KAFKA_LOG4J,
                )
                stack.callback(stop_process, kafka_proc)

                config_data = {
                    "zookeeper": asdict(zk_config),
                    "kafka": asdict(kafka_config),
                    WORKER_COUNTER_KEY: 1,
                }
                transfer_file.write_text(ujson.dumps(config_data))

        # Make sure every test worker can communicate with kafka
        kafka_servers = KafkaServers(bootstrap_servers=[f"127.0.0.1:{kafka_config.plaintext_port}"])
        wait_for_kafka(kafka_servers, KAFKA_WAIT_TIMEOUT)

        yield kafka_servers

        # Signal the worker finished
        with FileLock(str(lock_file)):
            assert transfer_file.exists(), "transfer_file disappeared"
            config_data = ujson.loads(transfer_file.read_text())
            config_data[WORKER_COUNTER_KEY] -= 1
            transfer_file.write_text(ujson.dumps(config_data))

        # Wait until every worker finished before stopping the servers
        worker_counter = float("inf")
        while worker_counter > 0:
            with FileLock(str(lock_file)):
                assert transfer_file.exists(), "transfer_file disappeared"
                config_data = ujson.loads(transfer_file.read_text())
                worker_counter = config_data[WORKER_COUNTER_KEY]

            time.sleep(2)

        return


@pytest.fixture(scope="function", name="producer")
def fixture_producer(kafka_servers: KafkaServers) -> KafkaProducer:
    with closing(KafkaProducer(bootstrap_servers=kafka_servers.bootstrap_servers)) as prod:
        yield prod


@pytest.fixture(scope="function", name="admin_client")
def fixture_admin(kafka_servers: KafkaServers) -> Iterator[KafkaRestAdminClient]:
    with closing(KafkaRestAdminClient(bootstrap_servers=kafka_servers.bootstrap_servers)) as cli:
        yield cli


@pytest.fixture(scope="function", name="rest_async")
async def fixture_rest_async(
    request: SubRequest,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
    tmp_path: Path,
    kafka_servers: KafkaServers,
    registry_async_client: Client,
) -> AsyncIterator[Optional[KafkaRest]]:

    # Do not start a REST api when the user provided an external service. Doing
    # so would cause this node to join the existing group and participate in
    # the election process. Without proper configuration for the listeners that
    # won't work and will cause test failures.
    rest_url = request.config.getoption("rest_url")
    if rest_url:
        yield None
        return

    config_path = tmp_path / "karapace_config.json"

    config = set_config_defaults({"bootstrap_uri": kafka_servers.bootstrap_servers, "admin_metadata_max_age": 2})
    write_config(config_path, config)
    rest = KafkaRest(config=config)

    assert rest.serializer.registry_client
    assert rest.consumer_manager.deserializer.registry_client
    rest.serializer.registry_client.client = registry_async_client
    rest.consumer_manager.deserializer.registry_client.client = registry_async_client
    try:
        yield rest
    finally:
        await rest.close()


@pytest.fixture(scope="function", name="rest_async_client")
async def fixture_rest_async_client(
    request: SubRequest,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
    rest_async: KafkaRest,
    aiohttp_client: AiohttpClient,
) -> AsyncIterator[Client]:
    rest_url = request.config.getoption("rest_url")

    # client and server_uri are incompatible settings.
    if rest_url:
        client = Client(server_uri=rest_url)
    else:

        async def get_client() -> ClientSession:
            return await aiohttp_client(rest_async.app)

        client = Client(client_factory=get_client)

    try:
        # wait until the server is listening, otherwise the tests may fail
        await repeat_until_successful_request(
            client.get,
            "brokers",
            json_data=None,
            headers=None,
            error_msg="REST API is unreachable",
            timeout=10,
            sleep=0.3,
        )
        yield client
    finally:
        await client.close()


@pytest.fixture(scope="function", name="registry_async_pair")
def fixture_registry_async_pair(
    tmp_path: Path,
    kafka_servers: KafkaServers,
) -> Iterator[Tuple[str, str]]:
    master_config_path = tmp_path / "karapace_config_master.json"
    slave_config_path = tmp_path / "karapace_config_slave.json"
    master_port = get_random_port(port_range=REGISTRY_PORT_RANGE, blacklist=[])
    slave_port = get_random_port(port_range=REGISTRY_PORT_RANGE, blacklist=[master_port])
    topic_name = new_random_name("schema_pairs")
    group_id = new_random_name("schema_pairs")
    write_config(
        master_config_path,
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "topic_name": topic_name,
            "group_id": group_id,
            "advertised_hostname": "127.0.0.1",
            "karapace_registry": True,
            "port": master_port,
        },
    )
    write_config(
        slave_config_path,
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "topic_name": topic_name,
            "group_id": group_id,
            "advertised_hostname": "127.0.0.1",
            "karapace_registry": True,
            "port": slave_port,
        },
    )

    master_process = None
    slave_process = None
    with ExitStack() as stack:
        try:
            master_process = stack.enter_context(Popen(["python", "-m", "karapace.karapace_all", str(master_config_path)]))
            slave_process = stack.enter_context(Popen(["python", "-m", "karapace.karapace_all", str(slave_config_path)]))
            wait_for_port_subprocess(master_port, master_process)
            wait_for_port_subprocess(slave_port, slave_process)
            yield f"http://127.0.0.1:{master_port}", f"http://127.0.0.1:{slave_port}"
        finally:
            if master_process:
                master_process.kill()
            if slave_process:
                slave_process.kill()


@pytest.fixture(scope="function", name="registry_async")
async def fixture_registry_async(
    request: SubRequest,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
    tmp_path: Path,
    kafka_servers: KafkaServers,
) -> AsyncIterator[Optional[KarapaceSchemaRegistry]]:
    # Do not start a registry when the user provided an external service. Doing
    # so would cause this node to join the existing group and participate in
    # the election process. Without proper configuration for the listeners that
    # won't work and will cause test failures.
    registry_url = request.config.getoption("registry_url")
    if registry_url:
        yield None
        return

    config_path = tmp_path / "karapace_config.json"

    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            # Using the default settings instead of random values, otherwise it
            # would not be possible to run the tests with external services.
            # Because of this every test must be written in such a way that it can
            # be executed twice with the same servers.
            # "topic_name": new_random_name("topic"),
            "group_id": new_random_name("registry_async"),
        }
    )
    write_config(config_path, config)
    registry = KarapaceSchemaRegistry(config=config)
    await registry.get_master()
    try:
        yield registry
    finally:
        await registry.close()


@pytest.fixture(scope="function", name="registry_async_client")
async def fixture_registry_async_client(
    request: SubRequest,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
    registry_async: KarapaceSchemaRegistry,
    aiohttp_client: AiohttpClient,
) -> AsyncIterator[Client]:

    registry_url = request.config.getoption("registry_url")

    # client and server_uri are incompatible settings.
    if registry_url:
        client = Client(server_uri=registry_url, server_ca=request.config.getoption("server_ca"))
    else:

        async def get_client() -> ClientSession:
            return await aiohttp_client(registry_async.app)

        client = Client(client_factory=get_client)

    try:
        # wait until the server is listening, otherwise the tests may fail
        await repeat_until_successful_request(
            client.get,
            "subjects",
            json_data=None,
            headers=None,
            error_msg="REST API is unreachable",
            timeout=10,
            sleep=0.3,
        )
        yield client
    finally:
        await client.close()


@pytest.fixture(scope="function", name="credentials_folder")
def fixture_credentials_folder() -> str:
    integration_test_folder = os.path.dirname(__file__)
    credentials_folder = os.path.join(integration_test_folder, "credentials")
    return credentials_folder


@pytest.fixture(scope="function", name="server_ca")
def fixture_server_ca(credentials_folder: str) -> str:
    return os.path.join(credentials_folder, "cacert.pem")


@pytest.fixture(scope="function", name="server_cert")
def fixture_server_cert(credentials_folder: str) -> str:
    return os.path.join(credentials_folder, "servercert.pem")


@pytest.fixture(scope="function", name="server_key")
def fixture_server_key(credentials_folder: str) -> str:
    return os.path.join(credentials_folder, "serverkey.pem")


@pytest.fixture(scope="function", name="registry_async_tls")
async def fixture_registry_async_tls(
    request: SubRequest,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
    tmp_path: Path,
    kafka_servers: KafkaServers,
    server_cert: str,
    server_key: str,
) -> AsyncIterator[Optional[KarapaceSchemaRegistry]]:
    # Do not start a registry when the user provided an external service. Doing
    # so would cause this node to join the existing group and participate in
    # the election process. Without proper configuration for the listeners that
    # won't work and will cause test failures.
    registry_url = request.config.getoption("registry_url")
    if registry_url:
        yield None
        return

    config_path = tmp_path / "karapace_config.json"

    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "server_tls_certfile": server_cert,
            "server_tls_keyfile": server_key,
            "port": 8444,
            # Using the default settings instead of random values, otherwise it
            # would not be possible to run the tests with external services.
            # Because of this every test must be written in such a way that it can
            # be executed twice with the same servers.
            # "topic_name": new_random_name("topic"),
            "group_id": new_random_name("registry_async_tls"),
        }
    )
    write_config(config_path, config)
    registry = KarapaceSchemaRegistry(config=config)
    await registry.get_master()
    try:
        yield registry
    finally:
        await registry.close()


@pytest.fixture(scope="function", name="registry_async_client_tls")
async def fixture_registry_async_client_tls(
    request: SubRequest,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
    registry_async_tls: KarapaceSchemaRegistry,
    aiohttp_client: AiohttpClient,
    server_ca: str,
) -> AsyncIterator[Client]:

    registry_url = request.config.getoption("registry_url")

    if registry_url:
        client = Client(server_uri=registry_url, server_ca=request.config.getoption("server_ca"))
    else:

        async def get_client() -> ClientSession:
            return await aiohttp_client(registry_async_tls.app)

        client = Client(client_factory=get_client, server_ca=server_ca)

    try:
        # wait until the server is listening, otherwise the tests may fail
        await repeat_until_successful_request(
            client.get,
            "subjects",
            json_data=None,
            headers=None,
            error_msg="REST API is unreachable",
            timeout=10,
            sleep=0.3,
        )
        yield client
    finally:
        await client.close()
