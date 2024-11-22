"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.security.base import SecurityBase
from karapace.auth.auth import AuthenticationError, AuthenticatorAndAuthorizer, HTTPAuthorizer, NoAuthAndAuthz, User
from schema_registry.dependencies.config_dependency import ConfigDependencyManager
from typing import Annotated, Optional

import logging

LOG = logging.getLogger(__name__)


class AuthorizationDependencyManager:
    AUTHORIZER: AuthenticatorAndAuthorizer | None = None
    AUTH_SET: bool = False
    SECURITY: SecurityBase | None = None

    @classmethod
    def get_authorizer(cls) -> AuthenticatorAndAuthorizer:
        if AuthorizationDependencyManager.AUTH_SET:
            assert AuthorizationDependencyManager.AUTHORIZER
            return AuthorizationDependencyManager.AUTHORIZER

        config = ConfigDependencyManager.get_config()
        if config.registry_authfile:
            AuthorizationDependencyManager.AUTHORIZER = HTTPAuthorizer(config.registry_authfile)
        else:
            # TODO: remove the need for empty authorization logic.
            AuthorizationDependencyManager.AUTHORIZER = NoAuthAndAuthz()
        AuthorizationDependencyManager.AUTH_SET = True
        return AuthorizationDependencyManager.AUTHORIZER


AuthenticatorAndAuthorizerDep = Annotated[AuthenticatorAndAuthorizer, Depends(AuthorizationDependencyManager.get_authorizer)]

# TODO Karapace can have authentication/authorization enabled or disabled. This code needs cleanup and better
# injection mechanism, this is fast workaround for optional user authentication and authorization.
SECURITY: SecurityBase | None = None
config = ConfigDependencyManager.get_config()
if config.registry_authfile:
    SECURITY = HTTPBasic(auto_error=False)

    def get_current_user(
        credentials: Annotated[Optional[HTTPBasicCredentials], Security(SECURITY)],
        authorizer: AuthenticatorAndAuthorizerDep,
    ) -> User:
        if authorizer and not credentials:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"message": "Unauthorized"},
                headers={"WWW-Authenticate": 'Basic realm="Karapace Schema Registry"'},
            )
        assert authorizer is not None
        assert credentials is not None
        username: str = credentials.username
        password: str = credentials.password
        try:
            return authorizer.authenticate(username=username, password=password)
        except AuthenticationError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"message": "Unauthorized"},
                headers={"WWW-Authenticate": 'Basic realm="Karapace Schema Registry"'},
            )

else:

    def get_current_user() -> None:
        return None


CurrentUserDep = Annotated[Optional[User], Depends(get_current_user)]
