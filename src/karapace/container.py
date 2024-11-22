"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector import containers, providers
from karapace.auth import get_authorizer, HTTPAuthorizer, NoAuthAndAuthz
from karapace.config import Config
from karapace.forward_client import ForwardClient
from karapace.instrumentation.prometheus import PrometheusInstrumentation
from karapace.schema_registry import KarapaceSchemaRegistry
from karapace.statsd import StatsClient


class KarapaceContainer(containers.DeclarativeContainer):
    base_config = providers.Configuration()
    config = providers.Singleton(
        Config,
        _env_file=base_config.karapace.env_file,
        _env_file_encoding=base_config.karapace.env_file_encoding,
    )

    statsd = providers.Singleton(StatsClient, config=config)

    no_auth_authorizer = providers.Singleton(NoAuthAndAuthz)

    http_authorizer = providers.Singleton(HTTPAuthorizer, config=config)

    schema_registry = providers.Singleton(KarapaceSchemaRegistry, config=config)

    forward_client = providers.Singleton(ForwardClient)

    authorizer = providers.Factory(
        get_authorizer,
        config=config,
        http_authorizer=http_authorizer,
        no_auth_authorizer=no_auth_authorizer,
    )

    prometheus = providers.Singleton(PrometheusInstrumentation)
