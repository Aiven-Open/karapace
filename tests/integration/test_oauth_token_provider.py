"""
End-to-end check of ``sasl_oauth_token_provider_class``.

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from _pytest.fixtures import SubRequest

from karapace.core.client import Client
from karapace.core.config import Config
from karapace.core.kafka.admin import KafkaAdminClient
from tests.integration.utils.cluster import after_master_is_available, start_schema_registry_cluster
from tests.integration.utils.kafka_server import KafkaServers
from tests.integration.utils.oauth_token_provider import UnsecuredJwtTokenProvider
from tests.utils import new_random_name


def _require_oauthbearer_listener(kafka_servers: KafkaServers) -> str:
    if kafka_servers.sasl_oauthbearer_bootstrap_servers is None:
        pytest.skip("Externally provided Kafka has no SASL/OAUTHBEARER listener")
    return kafka_servers.sasl_oauthbearer_bootstrap_servers[0]


def test_confluent_kafka_client_authenticates_with_token_provider(kafka_servers: KafkaServers) -> None:
    """Sanity check of the broker listener itself: librdkafka's `oauth_cb` path works."""
    bootstrap = _require_oauthbearer_listener(kafka_servers)

    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=UnsecuredJwtTokenProvider(),
    )
    assert admin.cluster_metadata()["brokers"]


async def test_registry_authenticates_to_kafka_with_token_provider(
    request: SubRequest,
    kafka_servers: KafkaServers,
    session_logdir: Path,
) -> None:
    """A registry configured with only a provider class (no static token) must elect a master
    and accept writes.

    Master election goes through aiokafka (``MasterCoordinator``), writes go through the
    registry's own confluent-kafka producer (``KarapaceProducer``); both must obtain their
    token from the configured provider or the registry never leaves the "no master" state.
    """
    bootstrap = _require_oauthbearer_listener(kafka_servers)

    config = Config()
    config.bootstrap_uri = bootstrap
    config.security_protocol = "SASL_PLAINTEXT"
    config.sasl_mechanism = "OAUTHBEARER"
    config.sasl_oauth_token_provider_class = UnsecuredJwtTokenProvider
    config.waiting_time_before_acting_as_master_ms = 500

    async with start_schema_registry_cluster(
        config_templates=[config],
        data_dir=session_logdir / request.node.name,
    ) as registries:
        async with after_master_is_available(registries, server_ca=None):
            client = Client(server_uri=registries[0].endpoint.to_url())
            try:
                subject = new_random_name("oauth-subject")
                res = await client.post_subjects_versions(
                    subject=subject,
                    json={"schema": json.dumps({"type": "string"})},
                )
                assert res.status_code == 200, res.json()
                schema_id = res.json()["id"]

                res = await client.get(f"schemas/ids/{schema_id}")
                assert res.status_code == 200, res.json()
                assert json.loads(res.json()["schema"]) == {"type": "string"}
            finally:
                await client.close()
