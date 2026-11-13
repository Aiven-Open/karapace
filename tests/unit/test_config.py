"""
Test config

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from copy import deepcopy
from dependency_injector import providers
from karapace.core.config import Config
from karapace.core.constants import (
    DEFAULT_AIOHTTP_CLIENT_MAX_SIZE,
    DEFAULT_OIDC_METHOD_ROLES,
    DEFAULT_PRODUCER_MAX_REQUEST,
)

import threading


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


class LockHoldingTokenProvider:
    """A provider that is not picklable, like one caching an SDK client or a session."""

    def __init__(self) -> None:
        self._lock = threading.Lock()

    def token_with_expiry(self, _config: str | None = None) -> tuple[str, int | None]:
        return ("token", None)


def _config_with_unpicklable_provider() -> Config:
    return Config(sasl_oauth_token_provider_class=LockHoldingTokenProvider)


def test_deepcopy_shares_oauth_token_provider_instance() -> None:
    config = _config_with_unpicklable_provider()
    provider = config._sasl_oauth_token_provider
    assert isinstance(provider, LockHoldingTokenProvider)

    clone = deepcopy(config)

    assert clone is not config
    assert clone._sasl_oauth_token_provider is provider


def test_set_config_defaults_keeps_oauth_token_provider_instance() -> None:
    config = _config_with_unpicklable_provider()
    provider = config._sasl_oauth_token_provider

    derived = config.set_config_defaults({"port": 9999})

    assert derived.port == 9999
    assert derived._sasl_oauth_token_provider is provider


def test_dependency_injector_can_copy_provider_with_config_argument() -> None:
    # dependency_injector deep-copies provider arguments while wiring containers; before
    # Config shared the token provider this raised NonCopyableArgumentError for any
    # provider holding a lock, a session or a client.
    config = _config_with_unpicklable_provider()
    singleton = providers.Singleton(dict, config=config)

    copied = deepcopy(singleton)

    assert copied()["config"]._sasl_oauth_token_provider is config._sasl_oauth_token_provider


def test_deepcopy_without_oauth_token_provider() -> None:
    config = Config()
    clone = deepcopy(config)
    assert clone is not config
    assert clone._sasl_oauth_token_provider is None
