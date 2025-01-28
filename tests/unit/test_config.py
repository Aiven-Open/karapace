"""
Test config

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from karapace.core.config import Config
from karapace.core.constants import DEFAULT_AIOHTTP_CLIENT_MAX_SIZE, DEFAULT_PRODUCER_MAX_REQUEST


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
