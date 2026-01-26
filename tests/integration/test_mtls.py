"""
karapace - schema tests

Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations


import aiohttp
import pytest
import ssl

from karapace.core.config import Config, ServerTLSClientAuth
from tests.integration.utils.cluster import start_schema_registry_cluster


@pytest.mark.asyncio
async def test_mtls_requires_client_certificate(
    tmp_path,
    kafka_servers,
    server_ca: str,
    server_cert: str,
    server_key: str,
) -> None:
    config = Config()
    config.bootstrap_uri = kafka_servers.bootstrap_servers[0]
    config.waiting_time_before_acting_as_master_ms = 500
    config.advertised_protocol = "https"
    config.server_tls_cafile = server_ca
    config.server_tls_certfile = server_cert
    config.server_tls_keyfile = server_key
    config.server_tls_client_auth = ServerTLSClientAuth.REQUIRED

    async with start_schema_registry_cluster(
        config_templates=[config],
        data_dir=tmp_path,
    ) as registries:
        endpoint = registries[0].endpoint.to_url()

        # Build an SSL context that trusts the server CA but does not present a client certificate.
        ssl_ctx = ssl.create_default_context(cafile=server_ca)
        ssl_ctx.check_hostname = False

        async with aiohttp.ClientSession() as session:
            failed = False
            resp = None
            try:
                resp = await session.get(f"{endpoint}/subjects", ssl=ssl_ctx)
                if resp.status >= 400:
                    failed = True
            except (aiohttp.ClientError, ssl.SSLError):
                failed = True
            finally:
                if resp is not None:
                    resp.release()
                    await resp.wait_for_close()

            assert failed, "mTLS required but request without client cert succeeded"


async def test_mtls_not_required_allows_without_client_certificate(
    tmp_path,
    kafka_servers,
    server_ca: str,
    server_cert: str,
    server_key: str,
) -> None:
    config = Config()
    config.bootstrap_uri = kafka_servers.bootstrap_servers[0]
    config.waiting_time_before_acting_as_master_ms = 500
    config.advertised_protocol = "https"
    config.server_tls_cafile = server_ca
    config.server_tls_certfile = server_cert
    config.server_tls_keyfile = server_key
    config.server_tls_client_auth = ServerTLSClientAuth.NONE

    async with start_schema_registry_cluster(
        config_templates=[config],
        data_dir=tmp_path,
    ) as registries:
        endpoint = registries[0].endpoint.to_url()

        # SSL context trusts the server CA and presents no client certificate.
        ssl_ctx = ssl.create_default_context(cafile=server_ca)
        ssl_ctx.check_hostname = False

        async with aiohttp.ClientSession() as session:
            async with session.get(f"{endpoint}/subjects", ssl=ssl_ctx) as resp:
                assert resp.status == 200
