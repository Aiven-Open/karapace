"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector import containers, providers
from karapace.core.auth import get_authorizer, HTTPAuthorizer, NoAuthAndAuthz
from karapace.core.config import Config
from karapace.api.forward_client import ForwardClient
from karapace.core.instrumentation.prometheus import PrometheusInstrumentation
from karapace.core.statsd import StatsClient


class KarapaceContainer(containers.DeclarativeContainer):
    config = providers.Singleton(Config)

    statsd = providers.Singleton(StatsClient, config=config)

    no_auth_authorizer = providers.Singleton(NoAuthAndAuthz)

    http_authorizer = providers.Singleton(HTTPAuthorizer, auth_file=config().registry_authfile)

    forward_client = providers.Singleton(ForwardClient)

    authorizer = providers.Factory(
        get_authorizer,
        config=config,
        http_authorizer=http_authorizer,
        no_auth_authorizer=no_auth_authorizer,
    )

    prometheus = providers.Singleton(PrometheusInstrumentation)
