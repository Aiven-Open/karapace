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
    asyncproducer = AsyncKafkaProducer(
        bootstrap_servers=kafka_servers.bootstrap_servers,
        client_id="asyncconsumer-1",
        loop=loop,
    )
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
def oidc_provider_name() -> str:
    provider = (os.environ.get("OIDC_PROVIDER") or "keycloak").strip().lower()
    if provider in {"pingfederate", "pingidentity"}:
        return "pingfederate"
    return "keycloak"


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_keycloak_oidc_token() -> str:
    admin_token = get_keycloak_admin_token()
    realm = os.environ.get("OIDC_REALM") or "karapace"
    client_id = os.environ.get("OIDC_CLIENT_ID") or "karapace-client"
    client_uuid = get_keycloak_client_uuid(realm, client_id, admin_token)
    client_secret = os.environ.get("OIDC_CLIENT_SECRET") or get_keycloak_client_secret(realm, client_uuid, admin_token)
    token_url = os.environ.get("OIDC_TOKEN_URL") or f"http://keycloak:8080/realms/{realm}/protocol/openid-connect/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": os.environ.get("OIDC_SCOPE") or "openid",
    }
    response = requests.post(token_url, data=data, timeout=30)
    response.raise_for_status()
    return response.json()["access_token"]


def _get_pingfederate_oidc_token() -> str:
    token_url = os.environ.get("OIDC_TOKEN_URL") or "https://pingfederate:9031/as/token.oauth2"
    client_secret = os.environ.get("OIDC_CLIENT_SECRET") or "karapace-secret"
    data = {
        "grant_type": "client_credentials",
        "client_id": os.environ.get("OIDC_CLIENT_ID") or "karapace-client",
        "client_secret": client_secret,
        "scope": os.environ.get("OIDC_SCOPE") or "openid",
    }
    response = requests.post(token_url, data=data, verify=_env_bool("OIDC_VERIFY_TLS", False), timeout=30)
    response.raise_for_status()
    return response.json()["access_token"]


@pytest.fixture(scope="session")
def oidc_token(oidc_provider_name: str) -> str:
    if oidc_provider_name == "pingfederate":
        return _get_pingfederate_oidc_token()
    return _get_keycloak_oidc_token()


@pytest.fixture(scope="function", name="registry_async_client_oidc")
async def fixture_registry_async_client_oidc(
    request: SubRequest,
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


@pytest.fixture(scope="function", name="registry_async_client_oidc_no_auth_header")
async def registry_async_client_oidc_no_auth_header(
    request: SubRequest,
    registry_cluster: RegistryDescription,
) -> AsyncGenerator[Client, None]:
    async def factory(auth):
        return ClientSession(headers={})

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


# OIDC SR with authentication only (karapace-schema-registry-authn-only, profile: e2e).


@pytest.fixture(scope="function", name="registry_authn_only_cluster")
async def fixture_registry_authn_only_cluster(
    loop: asyncio.AbstractEventLoop,
) -> RegistryDescription:
    endpoint = RegistryEndpoint("https", "karapace-schema-registry-authn-only", 8281)
    return RegistryDescription(endpoint, "_schemas_authn_only")


@pytest.fixture(scope="function", name="registry_async_client_oidc_authn_only")
async def fixture_registry_async_client_oidc_authn_only(
    request: SubRequest,
    registry_authn_only_cluster: RegistryDescription,
    loop: asyncio.AbstractEventLoop,
    oidc_token,
) -> AsyncGenerator[Client, None]:
    async def factory(auth):
        return ClientSession(headers={"Authorization": f"Bearer {oidc_token}"})

    client = Client(
        server_uri=registry_authn_only_cluster.endpoint.to_url(),
        server_ca=request.config.getoption("server_ca"),
        client_factory=factory,
        session_auth=None,
    )
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture(scope="function", name="registry_async_client_oidc_authn_only_invalid")
async def fixture_registry_async_client_oidc_authn_only_invalid(
    request: SubRequest,
    registry_authn_only_cluster: RegistryDescription,
    loop: asyncio.AbstractEventLoop,
) -> AsyncGenerator[Client, None]:
    async def factory(auth):
        return ClientSession(headers={"Authorization": "Bearer invalid_token"})

    client = Client(
        server_uri=registry_authn_only_cluster.endpoint.to_url(),
        server_ca=request.config.getoption("server_ca"),
        client_factory=factory,
        session_auth=None,
    )
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture(scope="function", name="registry_async_client_oidc_authn_only_no_auth_header")
async def fixture_registry_async_client_oidc_authn_only_no_auth_header(
    request: SubRequest,
    registry_authn_only_cluster: RegistryDescription,
) -> AsyncGenerator[Client, None]:
    async def factory(auth):
        return ClientSession(headers={})

    client = Client(
        server_uri=registry_authn_only_cluster.endpoint.to_url(),
        server_ca=request.config.getoption("server_ca"),
        client_factory=factory,
        session_auth=None,
    )
    try:
        yield client
    finally:
        await client.close()


# REST Proxy fixtures for OIDC forwarding tests (compose profile: e2e).
# -oidc has the gate ON; -no-forward has it OFF; both target the authn-only SR.


_OIDC_PROXY_URI = "http://karapace-rest-proxy-oidc:8382"
_NO_FORWARD_PROXY_URI = "http://karapace-rest-proxy-no-forward:8482"


def _make_proxy_client(server_uri: str, token: str | None) -> Client:
    factory_headers = {"Authorization": f"Bearer {token}"} if token is not None else {}

    async def factory(auth):
        return ClientSession(headers=factory_headers)

    return Client(
        server_uri=server_uri,
        client_factory=factory,
        session_auth=None,
    )


@pytest.fixture(scope="function", name="rest_async_client_oidc_proxy")
async def fixture_rest_async_client_oidc_proxy(
    loop: asyncio.AbstractEventLoop,
    oidc_token,
) -> AsyncGenerator[Client, None]:
    client = _make_proxy_client(_OIDC_PROXY_URI, oidc_token)
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture(scope="function", name="rest_async_client_oidc_proxy_invalid")
async def fixture_rest_async_client_oidc_proxy_invalid(
    loop: asyncio.AbstractEventLoop,
) -> AsyncGenerator[Client, None]:
    client = _make_proxy_client(_OIDC_PROXY_URI, "invalid_token")
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture(scope="function", name="rest_async_client_oidc_proxy_no_auth_header")
async def fixture_rest_async_client_oidc_proxy_no_auth_header(
    loop: asyncio.AbstractEventLoop,
) -> AsyncGenerator[Client, None]:
    client = _make_proxy_client(_OIDC_PROXY_URI, None)
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture(scope="function", name="rest_async_client_oidc_proxy_no_forward")
async def fixture_rest_async_client_oidc_proxy_no_forward(
    loop: asyncio.AbstractEventLoop,
    oidc_token,
) -> AsyncGenerator[Client, None]:
    """Valid Bearer aimed at a proxy with the gate OFF — proxy must not relay it."""
    client = _make_proxy_client(_NO_FORWARD_PROXY_URI, oidc_token)
    try:
        yield client
    finally:
        await client.close()


# Basic-auth SR + Basic proxy + gate OFF — issue #1274 baseline.


@pytest.fixture(scope="function", name="rest_async_client_basic_proxy")
async def fixture_rest_async_client_basic_proxy(
    loop: asyncio.AbstractEventLoop,
) -> AsyncGenerator[Client, None]:
    async def factory(auth):
        return ClientSession(headers={})

    client = Client(
        server_uri="http://karapace-rest-proxy-basic:8682",
        client_factory=factory,
        session_auth=None,
    )
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture(scope="function", name="registry_async_client_basic")
async def fixture_registry_async_client_basic(
    loop: asyncio.AbstractEventLoop,
) -> AsyncGenerator[Client, None]:
    """Direct client to the Basic-auth SR (used to wait for primary election)."""
    client = Client(
        server_uri="http://karapace-schema-registry-basic:8581",
        session_auth=BasicAuth("admin", "admin"),
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
def get_keycloak_admin_token():
    url = "http://keycloak:8080/realms/master/protocol/openid-connect/token"
    data = {
        "grant_type": "password",
        "client_id": "admin-cli",
        "username": os.environ.get("KEYCLOAK_ADMIN", "admin"),
        "password": os.environ.get("KEYCLOAK_ADMIN_PASSWORD", "admin"),
    }
    resp = requests.post(url, data=data, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_keycloak_client_uuid(realm, client_id, admin_token):
    url = f"http://keycloak:8080/admin/realms/{realm}/clients?clientId={client_id}"
    headers = {"Authorization": f"Bearer {admin_token}"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    clients = resp.json()
    return clients[0]["id"]


def get_keycloak_client_secret(realm, client_uuid, admin_token):
    url = f"http://keycloak:8080/admin/realms/{realm}/clients/{client_uuid}/client-secret"
    headers = {"Authorization": f"Bearer {admin_token}"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()["value"]
