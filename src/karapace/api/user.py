"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

import logging

from dependency_injector.wiring import inject, Provide
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from karapace.core.auth import AuthenticationError, AuthenticatorAndAuthorizer, HashAlgorithm, User
from typing import Annotated

from karapace.core.auth_container import AuthContainer

log = logging.getLogger(__name__)


@inject
async def get_current_user(
    request: Request,
    credentials: Annotated[HTTPBasicCredentials, Depends(HTTPBasic(auto_error=False))],
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[AuthContainer.authorizer]),
) -> User | None:
    # Check if OIDC authentication was used (set by OIDC middleware)
    if hasattr(request.state, "user") and request.state.user is not None:
        # OIDC authentication was successful, create a User from the JWT payload
        # The payload typically contains 'sub' (subject) claim
        # For OIDC, we create a User with dummy password fields since OIDC doesn't use passwords
        try:
            oidc_payload = request.state.user
            # Ensure oidc_payload is a dict-like object
            if not isinstance(oidc_payload, dict):
                # If it's not a dict, try to convert or use a default
                sub_claim = str(oidc_payload) if oidc_payload else ""
            else:
                sub_claim = oidc_payload.get("sub", "")
            # Create a User with dummy values for password fields (not used for OIDC)
            return User(
                username=sub_claim,
                algorithm=HashAlgorithm.SHA256,
                salt="",
                password_hash="",
            )
        except Exception as exc:
            # If there's any error processing OIDC payload, log and fall through to HTTP Basic auth
            log.warning("Error processing OIDC user payload: %s", exc, exc_info=True)
            # Fall through to HTTP Basic authentication

    # Fall back to HTTP Basic authentication
    if authorizer.MUST_AUTHENTICATE and not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"message": "Unauthorized"},
            headers={"WWW-Authenticate": 'Basic realm="Karapace Schema Registry"'},
        )
    username: str = credentials.username if credentials else ""
    password: str = credentials.password if credentials else ""
    try:
        return authorizer.authenticate(username=username, password=password)
    except AuthenticationError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"message": "Unauthorized"},
            headers={"WWW-Authenticate": 'Basic realm="Karapace Schema Registry"'},
        ) from exc
