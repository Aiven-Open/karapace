"""
Test config

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from karapace.config import set_config_defaults
from karapace.constants import DEFAULT_AIOHTTP_CLIENT_MAX_SIZE, DEFAULT_PRODUCER_MAX_REQUEST


def test_http_request_max_size() -> None:
    config = set_config_defaults(
        {
            "karapace_rest": False,
            "producer_max_request_size": DEFAULT_PRODUCER_MAX_REQUEST + 1024,
        }
    )
    assert config["http_request_max_size"] == DEFAULT_AIOHTTP_CLIENT_MAX_SIZE

    config = set_config_defaults(
        {
            "karapace_rest": False,
            "http_request_max_size": 1024,
        }
    )
    assert config["http_request_max_size"] == 1024

    config = set_config_defaults(
        {
            "karapace_rest": True,
        }
    )
    assert config["http_request_max_size"] == DEFAULT_AIOHTTP_CLIENT_MAX_SIZE

    config = set_config_defaults(
        {
            "karapace_rest": True,
            "producer_max_request_size": 1024,
        }
    )
    assert config["http_request_max_size"] == DEFAULT_AIOHTTP_CLIENT_MAX_SIZE

    config = set_config_defaults(
        {
            "karapace_rest": True,
            "producer_max_request_size": DEFAULT_PRODUCER_MAX_REQUEST + 1024,
        }
    )
    assert config["http_request_max_size"] == DEFAULT_PRODUCER_MAX_REQUEST + 1024 + DEFAULT_AIOHTTP_CLIENT_MAX_SIZE

    config = set_config_defaults(
        {
            "karapace_rest": True,
            "producer_max_request_size": DEFAULT_PRODUCER_MAX_REQUEST + 1024,
            "http_request_max_size": 1024,
        }
    )
    assert config["http_request_max_size"] == 1024
