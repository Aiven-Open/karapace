"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from karapace.auth import AuthenticationError, AuthenticatorAndAuthorizer, User
from schema_registry.container import SchemaRegistryContainer
from typing import Annotated


@inject
async def get_current_user(
    credentials: Annotated[HTTPBasicCredentials, Depends(HTTPBasic())],
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
) -> User:
    import logging

    logging.info("get_current_user ++++++++++++=============")
    logging.info(f"credentials: {credentials}")
    logging.info(f"authorizer: {authorizer}")
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
    except AuthenticationError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"message": "Unauthorized"},
            headers={"WWW-Authenticate": 'Basic realm="Karapace Schema Registry"'},
        ) from exc
