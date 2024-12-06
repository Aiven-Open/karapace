"""
karapace - conftest

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from _pytest.fixtures import SubRequest
from aiohttp import BasicAuth
from collections.abc import AsyncGenerator, Iterator
from confluent_kafka.admin import NewTopic
from karapace.client import Client
from karapace.container import KarapaceContainer
from karapace.kafka.admin import KafkaAdminClient
from karapace.kafka.consumer import AsyncKafkaConsumer, KafkaConsumer
from karapace.kafka.producer import AsyncKafkaProducer, KafkaProducer
from tests.integration.utils.cluster import RegistryDescription, RegistryEndpoint
from tests.integration.utils.kafka_server import KafkaServers

import asyncio
import pytest
import secrets


@pytest.fixture(scope="session", name="basic_auth")
def fixture_basic_auth() -> BasicAuth:
    return BasicAuth("test", "test")


@pytest.fixture(name="karapace_container", scope="session")
def fixture_karapace_container() -> KarapaceContainer:
    return KarapaceContainer()


@pytest.fixture(scope="session", name="kafka_servers")
def fixture_kafka_server(karapace_container: KarapaceContainer) -> Iterator[KafkaServers]:
    yield KafkaServers([karapace_container.config().bootstrap_uri])


@pytest.fixture(scope="function", name="producer")
def fixture_producer(kafka_servers: KafkaServers) -> Iterator[KafkaProducer]:
    yield KafkaProducer(bootstrap_servers=kafka_servers.bootstrap_servers)


@pytest.fixture(scope="function", name="admin_client")
def fixture_admin(kafka_servers: KafkaServers) -> Iterator[KafkaAdminClient]:
    yield KafkaAdminClient(bootstrap_servers=kafka_servers.bootstrap_servers)


@pytest.fixture(scope="function", name="consumer")
def fixture_consumer(
    kafka_servers: KafkaServers,
) -> Iterator[KafkaConsumer]:
    consumer = KafkaConsumer(
        bootstrap_servers=kafka_servers.bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        topic_metadata_refresh_interval_ms=200,  # Speed things up for consumer tests to discover topics, etc.
    )
    try:
        yield consumer
    finally:
        consumer.close()


@pytest.fixture(scope="function", name="asyncproducer")
async def fixture_asyncproducer(
    kafka_servers: KafkaServers,
    loop: asyncio.AbstractEventLoop,
) -> AsyncGenerator[AsyncKafkaProducer, None]:
    asyncproducer = AsyncKafkaProducer(bootstrap_servers=kafka_servers.bootstrap_servers, loop=loop)
    await asyncproducer.start()
    yield asyncproducer
    await asyncproducer.stop()


@pytest.fixture(scope="function", name="asyncconsumer")
async def fixture_asyncconsumer(
    kafka_servers: KafkaServers,
    loop: asyncio.AbstractEventLoop,
) -> AsyncGenerator[AsyncKafkaConsumer, None]:
    asyncconsumer = AsyncKafkaConsumer(
        bootstrap_servers=kafka_servers.bootstrap_servers,
        loop=loop,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        topic_metadata_refresh_interval_ms=200,  # Speed things up for consumer tests to discover topics, etc.
    )
    await asyncconsumer.start()
    yield asyncconsumer
    await asyncconsumer.stop()


@pytest.fixture(scope="function", name="registry_cluster")
async def fixture_registry_cluster(
    karapace_container: KarapaceContainer,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
) -> RegistryDescription:
    protocol = "http"
    endpoint = RegistryEndpoint(
        protocol, karapace_container.config().registry_host, karapace_container.config().registry_port
    )
    return RegistryDescription(endpoint, karapace_container.config().topic_name)


@pytest.fixture(scope="function", name="registry_async_client")
async def fixture_registry_async_client(
    request: SubRequest,
    basic_auth: BasicAuth,
    registry_cluster: RegistryDescription,
    loop: asyncio.AbstractEventLoop,  # pylint: disable=unused-argument
) -> AsyncGenerator[Client, None]:
    client = Client(
        server_uri=registry_cluster.endpoint.to_url(),
        server_ca=request.config.getoption("server_ca"),
        session_auth=basic_auth,
    )
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture(scope="function", name="new_topic")
def fixture_new_topic(admin_client: KafkaAdminClient) -> NewTopic:
    topic_name = secrets.token_hex(4)
    return admin_client.new_topic(topic_name, num_partitions=1, replication_factor=1)
