"""
Test config

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from karapace.core.config import Config, create_server_ssl_context, InvalidConfiguration
from karapace.core.constants import DEFAULT_AIOHTTP_CLIENT_MAX_SIZE, DEFAULT_PRODUCER_MAX_REQUEST
import pytest
import ssl
import tempfile
import os


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


def test_create_server_ssl_context_no_tls() -> None:
    """Test that no SSL context is created when TLS is not configured"""
    config = Config()
    config.server_tls_certfile = None
    config.server_tls_keyfile = None
    config.server_tls_cafile = None

    ssl_context = create_server_ssl_context(config)
    assert ssl_context is None


def test_create_server_ssl_context_missing_keyfile() -> None:
    """Test that an error is raised when certfile is provided but keyfile is missing"""
    config = Config()
    config.server_tls_certfile = "/path/to/cert.pem"
    config.server_tls_keyfile = None
    config.server_tls_cafile = None

    with pytest.raises(InvalidConfiguration, match="server_tls_certfile.*defined but.*server_tls_keyfile.*not defined"):
        create_server_ssl_context(config)


def test_create_server_ssl_context_missing_certfile() -> None:
    """Test that an error is raised when keyfile is provided but certfile is missing"""
    config = Config()
    config.server_tls_certfile = None
    config.server_tls_keyfile = "/path/to/key.pem"
    config.server_tls_cafile = None

    with pytest.raises(InvalidConfiguration, match="server_tls_keyfile.*defined but.*server_tls_certfile.*not defined"):
        create_server_ssl_context(config)


def test_create_server_ssl_context_invalid_certfile_type() -> None:
    """Test that an error is raised when certfile is not a string"""
    config = Config()
    config.server_tls_certfile = 123  # type: ignore
    config.server_tls_keyfile = "/path/to/key.pem"
    config.server_tls_cafile = None

    with pytest.raises(InvalidConfiguration, match="server_tls_certfile.*is not a string"):
        create_server_ssl_context(config)


def test_create_server_ssl_context_invalid_keyfile_type() -> None:
    """Test that an error is raised when keyfile is not a string"""
    config = Config()
    config.server_tls_certfile = "/path/to/cert.pem"
    config.server_tls_keyfile = 123  # type: ignore
    config.server_tls_cafile = None

    with pytest.raises(InvalidConfiguration, match="server_tls_keyfile.*is not a string"):
        create_server_ssl_context(config)


def test_create_server_ssl_context_nonexistent_certfile() -> None:
    """Test that an error is raised when certfile does not exist"""
    config = Config()
    config.server_tls_certfile = "/nonexistent/cert.pem"
    config.server_tls_keyfile = "/nonexistent/key.pem"
    config.server_tls_cafile = None

    with pytest.raises(InvalidConfiguration, match="server_tls_certfile.*file does not exist"):
        create_server_ssl_context(config)


def test_create_server_ssl_context_nonexistent_keyfile() -> None:
    """Test that an error is raised when keyfile does not exist"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.pem') as cert_file:
        cert_file.write("dummy cert")
        cert_path = cert_file.name

    try:
        config = Config()
        config.server_tls_certfile = cert_path
        config.server_tls_keyfile = "/nonexistent/key.pem"
        config.server_tls_cafile = None

        with pytest.raises(InvalidConfiguration, match="server_tls_keyfile.*file does not exist"):
            create_server_ssl_context(config)
    finally:
        os.unlink(cert_path)


def test_create_server_ssl_context_invalid_cafile_type() -> None:
    """Test that an error is raised when cafile is not a string"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.pem') as cert_file:
        cert_file.write("dummy cert")
        cert_path = cert_file.name

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.pem') as key_file:
        key_file.write("dummy key")
        key_path = key_file.name

    try:
        config = Config()
        config.server_tls_certfile = cert_path
        config.server_tls_keyfile = key_path
        config.server_tls_cafile = 123  # type: ignore

        with pytest.raises(InvalidConfiguration, match="server_tls_cafile.*is not a string"):
            create_server_ssl_context(config)
    finally:
        os.unlink(cert_path)
        os.unlink(key_path)


def test_create_server_ssl_context_nonexistent_cafile() -> None:
    """Test that an error is raised when cafile does not exist"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.pem') as cert_file:
        cert_file.write("dummy cert")
        cert_path = cert_file.name

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.pem') as key_file:
        key_file.write("dummy key")
        key_path = key_file.name

    try:
        config = Config()
        config.server_tls_certfile = cert_path
        config.server_tls_keyfile = key_path
        config.server_tls_cafile = "/nonexistent/ca.pem"

        with pytest.raises(InvalidConfiguration, match="server_tls_cafile.*file does not exist"):
            create_server_ssl_context(config)
    finally:
        os.unlink(cert_path)
        os.unlink(key_path)
