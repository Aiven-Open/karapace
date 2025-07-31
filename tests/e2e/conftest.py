"""
karapace - conftest

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import asyncio
import secrets
import time
from collections.abc import AsyncGenerator, Iterator

import pytest
from _pytest.fixtures import SubRequest
from aiohttp import BasicAuth, ClientSession
from confluent_kafka.admin import NewTopic
import requests
import os

from karapace.core.client import Client
from karapace.core.container import KarapaceContainer
from karapace.core.kafka.admin import KafkaAdminClient
from karapace.core.kafka.consumer import AsyncKafkaConsumer, KafkaConsumer
from karapace.core.kafka.producer import AsyncKafkaProducer, KafkaProducer
from tests.integration.utils.cluster import RegistryDescription, RegistryEndpoint
from tests.integration.utils.kafka_server import KafkaServers


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
    loop: asyncio.AbstractEventLoop,
) -> RegistryDescription:
    protocol = "https"
    endpoint = RegistryEndpoint(
        protocol, karapace_container.config().registry_host, karapace_container.config().registry_port
    )
    return RegistryDescription(endpoint, karapace_container.config().topic_name)


@pytest.fixture(scope="function", name="registry_async_client")
async def fixture_registry_async_client(
    request: SubRequest,
    basic_auth: BasicAuth,
    registry_cluster: RegistryDescription,
    loop: asyncio.AbstractEventLoop,
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


@pytest.fixture(scope="session")
def oidc_token():
    # --- Step 1: Get admin token ---
    admin_token = get_admin_token()

    # --- Step 2: Get client UUID ---
    realm = "karapace"
    client_id = "karapace-client"
    client_uuid = get_client_uuid(realm, client_id, admin_token)

    # --- Step 3: Get client secret ---
    client_secret = get_client_secret(realm, client_uuid, admin_token)

    # --- Step 4: Get OIDC token for the client ---
    token_url = f"http://keycloak:8080/realms/{realm}/protocol/openid-connect/token"
    data = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret, "scope": "openid"}
    response = requests.post(token_url, data=data)
    response.raise_for_status()
    return response.json()["access_token"]


@pytest.fixture(scope="function", name="registry_async_client_oidc")
async def fixture_registry_async_client_oidc(
    request: SubRequest,
    basic_auth: BasicAuth,
    registry_cluster: RegistryDescription,
    loop: asyncio.AbstractEventLoop,
    oidc_token,
) -> AsyncGenerator[Client, None]:
    async def factory(auth):
        return ClientSession(headers={"Authorization": f"Bearer {oidc_token}"})

    client = Client(
        server_uri=registry_cluster.endpoint.to_url(),
        server_ca=request.config.getoption("server_ca"),
        client_factory=factory,
        session_auth=None,
    )
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture(scope="function", name="registry_async_client_oidc_invalid")
async def fixture_registry_async_client_oidc_invalid(
    request: SubRequest,
    basic_auth: BasicAuth,
    registry_cluster: RegistryDescription,
    loop: asyncio.AbstractEventLoop,
    oidc_token,
) -> AsyncGenerator[Client, None]:
    async def factory(auth):
        return ClientSession(headers={"Authorization": "Bearer invalid_token"})

    client = Client(
        server_uri=registry_cluster.endpoint.to_url(),
        server_ca=request.config.getoption("server_ca"),
        client_factory=factory,
        session_auth=None,
    )
    try:
        yield client
    finally:
        await client.close()


class TokenProvider:
    def __init__(self, token: str, expiry_seconds: int = 3600):
        self._token = token
        self._expiry = int(time.time()) + expiry_seconds

    def token_with_expiry(self):
        return {
            "token": self._token,
            "expiry": self._expiry,
        }


# --- Helper functions for Keycloak admin API ---
def get_admin_token():
    url = "http://keycloak:8080/realms/master/protocol/openid-connect/token"
    data = {
        "grant_type": "password",
        "client_id": "admin-cli",
        "username": os.environ.get("KEYCLOAK_ADMIN", "admin"),
        "password": os.environ.get("KEYCLOAK_ADMIN_PASSWORD", "admin"),
    }
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_client_uuid(realm, client_id, admin_token):
    url = f"http://keycloak:8080/admin/realms/{realm}/clients?clientId={client_id}"
    headers = {"Authorization": f"Bearer {admin_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    clients = resp.json()
    return clients[0]["id"]


def get_client_secret(realm, client_uuid, admin_token):
    url = f"http://keycloak:8080/admin/realms/{realm}/clients/{client_uuid}/client-secret"
    headers = {"Authorization": f"Bearer {admin_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()["value"]
