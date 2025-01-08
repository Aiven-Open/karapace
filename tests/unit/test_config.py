"""
Test config

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from karapace.constants import DEFAULT_AIOHTTP_CLIENT_MAX_SIZE, DEFAULT_PRODUCER_MAX_REQUEST
from karapace.container import KarapaceContainer


def test_http_request_max_size(karapace_container: KarapaceContainer) -> None:
    config = karapace_container.config().set_config_defaults(
        {
            "karapace_rest": False,
            "producer_max_request_size": DEFAULT_PRODUCER_MAX_REQUEST + 1024,
        }
    )
    assert config.http_request_max_size == DEFAULT_AIOHTTP_CLIENT_MAX_SIZE

    config = karapace_container.config().set_config_defaults(
        {
            "karapace_rest": False,
            "http_request_max_size": 1024,
        }
    )
    assert config.http_request_max_size == 1024

    config = karapace_container.config().set_config_defaults(
        {
            "karapace_rest": True,
        }
    )
    assert config.http_request_max_size == DEFAULT_AIOHTTP_CLIENT_MAX_SIZE

    config = karapace_container.config().set_config_defaults(
        {
            "karapace_rest": True,
            "producer_max_request_size": 1024,
        }
    )
    assert config.http_request_max_size == DEFAULT_AIOHTTP_CLIENT_MAX_SIZE

    config = karapace_container.config().set_config_defaults(
        {
            "karapace_rest": True,
            "producer_max_request_size": DEFAULT_PRODUCER_MAX_REQUEST + 1024,
        }
    )
    assert config.http_request_max_size == DEFAULT_PRODUCER_MAX_REQUEST + 1024 + DEFAULT_AIOHTTP_CLIENT_MAX_SIZE

    config = karapace_container.config().set_config_defaults(
        {
            "karapace_rest": True,
            "producer_max_request_size": DEFAULT_PRODUCER_MAX_REQUEST + 1024,
            "http_request_max_size": 1024,
        }
    )
    assert config.http_request_max_size == 1024
