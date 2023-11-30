"""
karapace - conftest

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from _pytest.fixtures import SubRequest
from aiohttp.pytest_plugin import AiohttpClient
from aiohttp.test_utils import TestClient
from contextlib import ExitStack
from dataclasses import asdict
from filelock import FileLock
from karapace.client import Client
from karapace.config import Config, set_config_defaults, write_config
from karapace.kafka.admin import KafkaAdminClient, NewTopic
from karapace.kafka.producer import KafkaProducer
from karapace.kafka_rest_apis import KafkaRest
from pathlib import Path
from tests.conftest import KAFKA_VERSION
from tests.integration.utils.cluster import RegistryDescription, RegistryEndpoint, start_schema_registry_cluster
from tests.integration.utils.config import KafkaConfig, KafkaDescription, ZKConfig
from tests.integration.utils.kafka_server import (
    configure_and_start_kafka,
    KafkaServers,
    maybe_download_kafka,
    wait_for_kafka,
)
from tests.integration.utils.network import PortRangeInclusive
from tests.integration.utils.process import stop_process, wait_for_port_subprocess
from tests.integration.utils.synchronization import lock_path_for
from tests.integration.utils.zookeeper import configure_and_start_zk
from tests.utils import repeat_until_successful_request
from typing import AsyncIterator, Iterator, List, Optional
from urllib.parse import urlparse

import asyncio
import json
import os
import pathlib
import pytest
import re
import secrets
import string
import time

REPOSITORY_DIR = pathlib.Path(__file__).parent.parent.parent.absolute()
RUNTIME_DIR = REPOSITORY_DIR / "runtime"
TEST_INTEGRATION_DIR = REPOSITORY_DIR / "tests" / "integration"
KAFKA_WAIT_TIMEOUT = 60
KAFKA_SCALA_VERSION = "2.13"
# stdout logger is disabled to keep the pytest report readable
KAFKA_LOG4J = TEST_INTEGRATION_DIR / "config" / "log4j.properties"
WORKER_COUNTER_KEY = "worker_counter"

REST_PRODUCER_MAX_REQUEST_BYTES = 10240


def _clear_test_name(name: str) -> str:
    # Based on:
    # https://github.com/pytest-dev/pytest/blob/238b25ffa9d4acbc7072ac3dd6d8240765643aed/src/_pytest/tmpdir.py#L189-L194
    # The purpose is to return a similar name to make finding matching logs easier
    return re.sub(r"[\W]", "_", name)[:30]


@pytest.fixture(scope="session", name="port_range")
def fixture_port_range() -> PortRangeInclusive:
    """Container used by other fixtures to register used ports"""
    # To find a good port range use the following:
    #
    #   curl --silent 'https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.txt' | \
    #       egrep -i -e '^\s*[0-9]+-[0-9]+\s*unassigned' | \
    #       awk '{print $1}'
    #
    start = 48700
    end = 49000

    # Split the ports among the workers to prevent port reuse
    worker_name = os.environ.get("PYTEST_XDIST_WORKER", "0")
    worker_id = int(worker_name.lstrip(string.ascii_letters))
    worker_count = int(os.environ.get("PYTEST_XDIST_WORKER_COUNT", "1"))
    total_ports = end - start
    ports_per_worker = total_ports // worker_count
    start_worker = (ports_per_worker * worker_id) + start
    end_worker = start_worker + ports_per_worker - 1
    return PortRangeInclusive(start_worker, end_worker)


@pytest.fixture(scope="session", name="kafka_description")
def fixture_kafka_description(request: SubRequest) -> KafkaDescription:
    kafka_version = request.config.getoption("kafka_version") or KAFKA_VERSION
    kafka_folder = f"kafka_{KAFKA_SCALA_VERSION}-{kafka_version}"
    kafka_tgz = f"{kafka_folder}.tgz"
    kafka_url = f"https://archive.apache.org/dist/kafka/{kafka_version}/{kafka_tgz}"
    kafka_dir = RUNTIME_DIR / kafka_folder

    return KafkaDescription(
        version=kafka_version,
        kafka_tgz=RUNTIME_DIR / kafka_tgz,
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
    port_range: PortRangeInclusive,
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
        zk_client_port = stack.enter_context(port_range.allocate_port())
        zk_admin_port = stack.enter_context(port_range.allocate_port())
        kafka_plaintext_port = stack.enter_context(port_range.allocate_port())

        with FileLock(str(lock_file)):
            if transfer_file.exists():
                config_data = json.loads(transfer_file.read_text())
                zk_config = ZKConfig.from_dict(config_data["zookeeper"])
                kafka_config = KafkaConfig.from_dict(config_data["kafka"])
                config_data[WORKER_COUNTER_KEY] += 1  # Count the new worker
            else:
                maybe_download_kafka(kafka_description)

                zk_config = ZKConfig(
                    client_port=zk_client_port,
                    admin_port=zk_admin_port,
                    path=str(zk_dir),
                )

                zk_proc = configure_and_start_zk(zk_config, kafka_description)
                stack.callback(stop_process, zk_proc)

                # Make sure zookeeper is running before trying to start Kafka
                wait_for_port_subprocess(zk_config.client_port, zk_proc, wait_time=20)

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

            transfer_file.write_text(json.dumps(config_data))

        try:
            # Make sure every test worker can communicate with kafka
            kafka_servers = KafkaServers(bootstrap_servers=[f"127.0.0.1:{kafka_config.plaintext_port}"])
            wait_for_kafka(kafka_servers, KAFKA_WAIT_TIMEOUT)

            yield kafka_servers
        finally:
            # This must be called on errors, otherwise the master node will wait forever
            with FileLock(str(lock_file)):
                assert transfer_file.exists(), "transfer_file disappeared"
                config_data = json.loads(transfer_file.read_text())
                config_data[WORKER_COUNTER_KEY] -= 1
                transfer_file.write_text(json.dumps(config_data))

            # Wait until every worker finished before stopping the servers
            worker_counter = float("inf")
            while worker_counter > 0:
                with FileLock(str(lock_file)):
                    assert transfer_file.exists(), "transfer_file disappeared"
                    config_data = json.loads(transfer_file.read_text())
                    worker_counter = config_data[WORKER_COUNTER_KEY]

                time.sleep(2)

        return


@pytest.fixture(scope="function", name="producer")
def fixture_producer(kafka_servers: KafkaServers) -> KafkaProducer:
    yield KafkaProducer(bootstrap_servers=kafka_servers.bootstrap_servers)


@pytest.fixture(scope="function", name="admin_client")
def fixture_admin(kafka_servers: KafkaServers) -> Iterator[KafkaAdminClient]:
    yield KafkaAdminClient(bootstrap_servers=kafka_servers.bootstrap_servers)


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

    config = set_config_defaults(
        {
            "admin_metadata_max_age": 2,
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            # Use non-default max request size for REST producer.
            "producer_max_request_size": REST_PRODUCER_MAX_REQUEST_BYTES,
        }
    )
    write_config(config_path, config)
    rest = KafkaRest(config=config)

    assert rest.serializer.registry_client
    rest.serializer.registry_client.client = registry_async_client
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

        async def get_client(**kwargs) -> TestClient:  # pylint: disable=unused-argument
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


@pytest.fixture(scope="function", name="rest_async_novalidation")
async def fixture_rest_async_novalidation(
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

    config = set_config_defaults(
        {
            "admin_metadata_max_age": 2,
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            # Use non-default max request size for REST producer.
            "producer_max_request_size": REST_PRODUCER_MAX_REQUEST_BYTES,
            "name_strategy_validation": False,  # This should be only difference from rest_async
        }
    )
    write_config(config_path, config)
    rest = KafkaRest(config=config)

    assert rest.serializer.registry_client
    rest.serializer.registry_client.client = registry_async_client
    try:
        yield rest
    finally:
        await rest.close()


@pytest.fixture(scope="function", name="rest_async_novalidation_client")
async def fixture_rest_async_novalidationclient(
    request: SubRequest,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
    rest_async_novalidation: KafkaRest,
    aiohttp_client: AiohttpClient,
) -> AsyncIterator[Client]:
    rest_url = request.config.getoption("rest_url")

    # client and server_uri are incompatible settings.
    if rest_url:
        client = Client(server_uri=rest_url)
    else:

        async def get_client(**kwargs) -> TestClient:  # pylint: disable=unused-argument
            return await aiohttp_client(rest_async_novalidation.app)

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


@pytest.fixture(scope="function", name="rest_async_registry_auth")
async def fixture_rest_async_registry_auth(
    request: SubRequest,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
    kafka_servers: KafkaServers,
    registry_async_client_auth: Client,
) -> AsyncIterator[Optional[KafkaRest]]:
    # Do not start a REST api when the user provided an external service. Doing
    # so would cause this node to join the existing group and participate in
    # the election process. Without proper configuration for the listeners that
    # won't work and will cause test failures.
    rest_url = request.config.getoption("rest_url")
    if rest_url:
        yield None
        return

    registry = urlparse(registry_async_client_auth.server_uri)
    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "admin_metadata_max_age": 2,
            "registry_host": registry.hostname,
            "registry_port": registry.port,
            "registry_user": "admin",
            "registry_password": "admin",
        }
    )
    rest = KafkaRest(config=config)

    try:
        yield rest
    finally:
        await rest.close()


@pytest.fixture(scope="function", name="rest_async_client_registry_auth")
async def fixture_rest_async_client_registry_auth(
    request: SubRequest,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
    rest_async_registry_auth: KafkaRest,
    aiohttp_client: AiohttpClient,
) -> AsyncIterator[Client]:
    rest_url = request.config.getoption("rest_url")

    # client and server_uri are incompatible settings.
    if rest_url:
        client = Client(server_uri=rest_url)
    else:

        async def get_client(**kwargs) -> TestClient:  # pylint: disable=unused-argument
            return await aiohttp_client(rest_async_registry_auth.app)

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
async def fixture_registry_async_pair(
    request: SubRequest,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
    session_logdir: Path,
    kafka_servers: KafkaServers,
    port_range: PortRangeInclusive,
) -> AsyncIterator[List[str]]:
    """Starts a cluster of two Schema Registry servers and returns their URL endpoints."""

    config1: Config = {"bootstrap_uri": kafka_servers.bootstrap_servers}
    config2: Config = {"bootstrap_uri": kafka_servers.bootstrap_servers}

    async with start_schema_registry_cluster(
        config_templates=[config1, config2],
        data_dir=session_logdir / _clear_test_name(request.node.name),
        port_range=port_range,
    ) as endpoints:
        yield [server.endpoint.to_url() for server in endpoints]


@pytest.fixture(scope="function", name="registry_cluster")
async def fixture_registry_cluster(
    request: SubRequest,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
    session_logdir: Path,
    kafka_servers: KafkaServers,
    port_range: PortRangeInclusive,
) -> AsyncIterator[RegistryDescription]:
    # Do not start a registry when the user provided an external service. Doing
    # so would cause this node to join the existing group and participate in
    # the election process. Without proper configuration for the listeners that
    # won't work and will cause test failures.
    registry_url = request.config.getoption("registry_url")
    if registry_url:
        registry = urlparse(registry_url)
        endpoint = RegistryEndpoint(registry.scheme, registry.hostname, registry.port)
        yield RegistryDescription(endpoint, "_schemas")
        return

    config = {"bootstrap_uri": kafka_servers.bootstrap_servers}
    async with start_schema_registry_cluster(
        config_templates=[config],
        data_dir=session_logdir / _clear_test_name(request.node.name),
        port_range=port_range,
    ) as servers:
        yield servers[0]


@pytest.fixture(scope="function", name="registry_async_client")
async def fixture_registry_async_client(
    request: SubRequest,
    registry_cluster: RegistryDescription,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
) -> Client:
    client = Client(
        server_uri=registry_cluster.endpoint.to_url(),
        server_ca=request.config.getoption("server_ca"),
    )

    try:
        # wait until the server is listening, otherwise the tests may fail
        await repeat_until_successful_request(
            client.get,
            "subjects",
            json_data=None,
            headers=None,
            error_msg=f"Registry API {client.server_uri} is unreachable",
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


@pytest.fixture(scope="function", name="registry_https_endpoint")
async def fixture_registry_https_endpoint(
    request: SubRequest,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
    session_logdir: Path,
    kafka_servers: KafkaServers,
    server_cert: str,
    server_key: str,
    port_range: PortRangeInclusive,
) -> AsyncIterator[str]:
    # Do not start a registry when the user provided an external service. Doing
    # so would cause this node to join the existing group and participate in
    # the election process. Without proper configuration for the listeners that
    # won't work and will cause test failures.
    registry_url = request.config.getoption("registry_url")
    if registry_url:
        yield registry_url
        return

    config = {
        "bootstrap_uri": kafka_servers.bootstrap_servers,
        "server_tls_certfile": server_cert,
        "server_tls_keyfile": server_key,
    }
    async with start_schema_registry_cluster(
        config_templates=[config],
        data_dir=session_logdir / _clear_test_name(request.node.name),
        port_range=port_range,
    ) as servers:
        yield servers[0].endpoint.to_url()


@pytest.fixture(scope="function", name="registry_async_client_tls")
async def fixture_registry_async_client_tls(
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
    registry_https_endpoint: str,
    server_ca: str,
) -> AsyncIterator[Client]:
    pytest.skip("Test certification is not properly set")

    client = Client(
        server_uri=registry_https_endpoint,
        server_ca=server_ca,
    )

    try:
        # wait until the server is listening, otherwise the tests may fail
        await repeat_until_successful_request(
            client.get,
            "subjects",
            json_data=None,
            headers=None,
            error_msg=f"Registry API {registry_https_endpoint} is unreachable",
            timeout=10,
            sleep=0.3,
        )
        yield client
    finally:
        await client.close()


@pytest.fixture(scope="function", name="registry_http_auth_endpoint")
async def fixture_registry_http_auth_endpoint(
    request: SubRequest,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
    session_logdir: Path,
    kafka_servers: KafkaServers,
    port_range: PortRangeInclusive,
) -> AsyncIterator[str]:
    # Do not start a registry when the user provided an external service. Doing
    # so would cause this node to join the existing group and participate in
    # the election process. Without proper configuration for the listeners that
    # won't work and will cause test failures.
    registry_url = request.config.getoption("registry_url")
    if registry_url:
        yield registry_url
        return

    config = {
        "bootstrap_uri": kafka_servers.bootstrap_servers,
        "registry_authfile": "tests/integration/config/karapace.auth.json",
    }
    async with start_schema_registry_cluster(
        config_templates=[config],
        data_dir=session_logdir / _clear_test_name(request.node.name),
        port_range=port_range,
    ) as servers:
        yield servers[0].endpoint.to_url()


@pytest.fixture(scope="function", name="registry_async_client_auth")
async def fixture_registry_async_client_auth(
    request: SubRequest,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
    registry_http_auth_endpoint: str,
) -> AsyncIterator[Client]:
    client = Client(
        server_uri=registry_http_auth_endpoint,
        server_ca=request.config.getoption("server_ca"),
    )

    try:
        # wait until the server is listening, otherwise the tests may fail
        await repeat_until_successful_request(
            client.get,
            "schemas/types",
            json_data=None,
            headers=None,
            error_msg=f"Registry API {registry_http_auth_endpoint} is unreachable",
            timeout=10,
            sleep=0.3,
        )
        yield client
    finally:
        await client.close()


@pytest.fixture(scope="function", name="registry_async_auth_pair")
async def fixture_registry_async_auth_pair(
    request: SubRequest,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
    session_logdir: Path,
    kafka_servers: KafkaServers,
    port_range: PortRangeInclusive,
) -> AsyncIterator[List[str]]:
    """Starts a cluster of two Schema Registry servers with authentication enabled and returns their URL endpoints."""

    config1: Config = {
        "bootstrap_uri": kafka_servers.bootstrap_servers,
        "registry_authfile": "tests/integration/config/karapace.auth.json",
    }
    config2: Config = {
        "bootstrap_uri": kafka_servers.bootstrap_servers,
        "registry_authfile": "tests/integration/config/karapace.auth.json",
    }

    async with start_schema_registry_cluster(
        config_templates=[config1, config2],
        data_dir=session_logdir / _clear_test_name(request.node.name),
        port_range=port_range,
    ) as endpoints:
        yield [server.endpoint.to_url() for server in endpoints]


@pytest.fixture(scope="function", name="new_topic")
def topic_fixture(admin_client: KafkaAdminClient) -> NewTopic:
    topic_name = secrets.token_hex(4)
    try:
        yield admin_client.new_topic(topic_name, num_partitions=1, replication_factor=1)
    finally:
        admin_client.delete_topic(topic_name)
