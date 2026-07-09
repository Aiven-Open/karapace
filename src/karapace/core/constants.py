"""
karapace - constants

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from typing import Final

SCHEMA_TOPIC_NUM_PARTITIONS: Final = 1
TOPIC_CREATION_TIMEOUT_S: Final = 20
DEFAULT_SCHEMA_TOPIC: Final = "_schemas"
DEFAULT_PRODUCER_MAX_REQUEST: Final = 1048576
DEFAULT_AIOHTTP_CLIENT_MAX_SIZE: Final = 1048576

SECOND: Final = 1000
MINUTE: Final = 60 * SECOND

# OIDC authorization role names. Must match the roles the IdP mints into tokens.
OIDC_ROLE_PREFIX: Final = "karapace."
OIDC_ROLE_SCHEMA_READ: Final = f"{OIDC_ROLE_PREFIX}schema:read"
OIDC_ROLE_SCHEMA_WRITE: Final = f"{OIDC_ROLE_PREFIX}schema:write"
OIDC_ROLE_SCHEMA_DELETE: Final = f"{OIDC_ROLE_PREFIX}schema:delete"
OIDC_ROLE_SUBJECT_READ: Final = f"{OIDC_ROLE_PREFIX}subject:read"
OIDC_ROLE_SUBJECT_WRITE: Final = f"{OIDC_ROLE_PREFIX}subject:write"
OIDC_ROLE_SUBJECT_DELETE: Final = f"{OIDC_ROLE_PREFIX}subject:delete"
OIDC_ROLE_CONFIG_SUBJECT_UPDATE: Final = f"{OIDC_ROLE_PREFIX}config_subject:update"
OIDC_ROLE_CONFIG_GLOBAL_UPDATE: Final = f"{OIDC_ROLE_PREFIX}config_global:update"

# Default OIDC method → allowed roles mapping. Mirrors the reference Keycloak realm.
DEFAULT_OIDC_METHOD_ROLES: Final[dict[str, list[str]]] = {
    "GET": [OIDC_ROLE_SCHEMA_READ, OIDC_ROLE_SUBJECT_READ],
    "POST": [OIDC_ROLE_SCHEMA_WRITE, OIDC_ROLE_SUBJECT_WRITE],
    "PUT": [OIDC_ROLE_CONFIG_SUBJECT_UPDATE, OIDC_ROLE_CONFIG_GLOBAL_UPDATE],
    "DELETE": [OIDC_ROLE_SCHEMA_DELETE, OIDC_ROLE_SUBJECT_DELETE],
}
