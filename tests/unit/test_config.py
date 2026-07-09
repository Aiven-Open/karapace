"""
Test config

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from karapace.core.config import Config
from karapace.core.constants import (
    DEFAULT_AIOHTTP_CLIENT_MAX_SIZE,
    DEFAULT_OIDC_METHOD_ROLES,
    DEFAULT_PRODUCER_MAX_REQUEST,
)


def test_http_request_max_size() -> None:
    config = Config()
    config.karapace_rest = False
    config.producer_max_request_size = DEFAULT_PRODUCER_MAX_REQUEST + 1024
    assert config.get_max_request_size() == DEFAULT_AIOHTTP_CLIENT_MAX_SIZE

    config = Config()
    config.karapace_rest = False
    config.http_request_max_size = 1024
    assert config.get_max_request_size() == 1024

    config = Config()
    config.karapace_rest = True
    assert config.get_max_request_size() == DEFAULT_AIOHTTP_CLIENT_MAX_SIZE

    config = Config()
    config.karapace_rest = True
    config.producer_max_request_size = 1024
    assert config.get_max_request_size() == DEFAULT_AIOHTTP_CLIENT_MAX_SIZE

    config = Config()
    config.karapace_rest = True
    config.producer_max_request_size = DEFAULT_PRODUCER_MAX_REQUEST + 1024
    assert config.get_max_request_size() == DEFAULT_PRODUCER_MAX_REQUEST + 1024 + DEFAULT_AIOHTTP_CLIENT_MAX_SIZE

    config = Config()
    config.karapace_rest = True
    config.http_request_max_size = 1024
    config.producer_max_request_size = DEFAULT_PRODUCER_MAX_REQUEST + 1024
    assert config.get_max_request_size() == 1024


def test_method_roles_default_uses_prefixed_roles() -> None:
    config = Config()
    assert config.sasl_oauthbearer_method_roles == DEFAULT_OIDC_METHOD_ROLES
    # All default role names carry the karapace. prefix.
    all_roles = [role for roles in config.sasl_oauthbearer_method_roles.values() for role in roles]
    assert all_roles and all(role.startswith("karapace.") for role in all_roles)


def test_method_roles_default_is_isolated_per_instance() -> None:
    # default_factory must deep-copy so mutating one Config never leaks into another.
    a = Config()
    b = Config()
    a.sasl_oauthbearer_method_roles["GET"].append("karapace.extra:role")
    assert "karapace.extra:role" not in b.sasl_oauthbearer_method_roles["GET"]
    assert "karapace.extra:role" not in DEFAULT_OIDC_METHOD_ROLES["GET"]
