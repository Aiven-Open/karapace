"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

from dependency_injector import containers, providers
from karapace.core.auth import get_authorizer, HTTPAuthorizer, NoAuthAndAuthz
from karapace.core.container import KarapaceContainer


# def create_http_authorizer(config: Config) -> HTTPAuthorizer:
#    return HTTPAuthorizer(auth_file=config.registry_authfile)


class AuthContainer(containers.DeclarativeContainer):
    karapace_container = providers.Container(KarapaceContainer)
    no_auth_authorizer = providers.Singleton(NoAuthAndAuthz)
    http_authorizer = providers.Singleton(HTTPAuthorizer, auth_file=karapace_container.config().registry_authfile)
    # http_authorizer = providers.Singleton(HTTPAuthorizer, create_http_authorizer)
    authorizer = providers.Factory(
        get_authorizer,
        config=karapace_container.config,
        http_authorizer=http_authorizer,
        no_auth_authorizer=no_auth_authorizer,
    )
