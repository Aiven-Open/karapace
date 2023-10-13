"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from http import HTTPStatus
from karapace.config import ConfigDefaults, set_config_defaults
from karapace.kafka_rest_apis.authentication import (
    get_auth_config_from_header,
    get_expiration_time_from_header,
    get_kafka_client_auth_parameters_from_config,
    SimpleOauthTokenProvider,
    SimpleOauthTokenProviderAsync,
)
from karapace.rapu import HTTPResponse, JSON_CONTENT_TYPE

import base64
import datetime
import jwt
import pytest


@pytest.mark.parametrize(
    "auth_header",
    (None, "Digest foo=bar"),
)
def test_get_auth_config_from_header_raises_unauthorized_on_invalid_header(auth_header: str | None) -> None:
    config = set_config_defaults({})

    with pytest.raises(HTTPResponse) as exc_info:
        get_auth_config_from_header(auth_header, config)

    http_resonse = exc_info.value
    assert http_resonse.body == '{"message": "Unauthorized"}'
    assert http_resonse.status == HTTPStatus.UNAUTHORIZED
    assert http_resonse.headers["Content-Type"] == JSON_CONTENT_TYPE
    assert http_resonse.headers["WWW-Authenticate"] == 'Basic realm="Karapace REST Proxy"'


@pytest.mark.parametrize(
    ("auth_header", "config_override", "expected_auth_config"),
    (
        (
            f"Basic {base64.b64encode(b'username:password').decode()}",
            {"sasl_mechanism": None},
            {"sasl_mechanism": "PLAIN", "sasl_plain_username": "username", "sasl_plain_password": "password"},
        ),
        (
            f"Basic {base64.b64encode(b'username:password').decode()}",
            {"sasl_mechanism": "PLAIN"},
            {"sasl_mechanism": "PLAIN", "sasl_plain_username": "username", "sasl_plain_password": "password"},
        ),
        (
            f"Basic {base64.b64encode(b'username:password').decode()}",
            {"sasl_mechanism": "SCRAM"},
            {"sasl_mechanism": "SCRAM", "sasl_plain_username": "username", "sasl_plain_password": "password"},
        ),
        (
            "Bearer <TOKEN>",
            {},
            {"sasl_mechanism": "OAUTHBEARER", "sasl_oauth_token": "<TOKEN>"},
        ),
    ),
)
def test_get_auth_config_from_header(
    auth_header: str, config_override: ConfigDefaults, expected_auth_config: ConfigDefaults
) -> None:
    config = set_config_defaults(config_override)
    auth_config = get_auth_config_from_header(auth_header, config)
    assert auth_config == expected_auth_config


@pytest.mark.parametrize(
    ("auth_header", "expected_expiration"),
    (
        (f"Basic {base64.b64encode(b'username:password').decode()}", None),
        (
            f"Bearer {jwt.encode({'exp': 1697013997}, 'secret')}",
            datetime.datetime.fromtimestamp(1697013997, datetime.timezone.utc),
        ),
        (f"Bearer {jwt.encode({}, 'secret')}", None),
    ),
)
def test_get_expiration_time_from_header(auth_header: str, expected_expiration: datetime.datetime) -> None:
    expiration = get_expiration_time_from_header(auth_header)

    assert expiration == expected_expiration


def test_simple_oauth_token_provider_returns_configured_token() -> None:
    token_provider = SimpleOauthTokenProvider("TOKEN")
    assert token_provider.token() == "TOKEN"


async def test_simple_oauth_token_provider_async_returns_configured_token() -> None:
    token_provider = SimpleOauthTokenProviderAsync("TOKEN")
    assert await token_provider.token() == "TOKEN"


def test_get_client_auth_parameters_from_config_sasl_plain() -> None:
    config = set_config_defaults(
        {"sasl_mechanism": "PLAIN", "sasl_plain_username": "username", "sasl_plain_password": "password"}
    )

    client_auth_params = get_kafka_client_auth_parameters_from_config(config)

    assert client_auth_params == {
        "sasl_mechanism": "PLAIN",
        "sasl_plain_username": "username",
        "sasl_plain_password": "password",
    }


def test_get_client_auth_parameters_from_config_oauth() -> None:
    config = set_config_defaults({"sasl_mechanism": "OAUTHBEARER", "sasl_oauth_token": "TOKEN"})

    client_auth_params = get_kafka_client_auth_parameters_from_config(config, async_client=False)

    assert client_auth_params["sasl_mechanism"] == "OAUTHBEARER"
    assert client_auth_params["sasl_oauth_token_provider"].token() == "TOKEN"


async def test_get_client_auth_parameters_from_config_oauth_async() -> None:
    config = set_config_defaults({"sasl_mechanism": "OAUTHBEARER", "sasl_oauth_token": "TOKEN"})

    client_auth_params = get_kafka_client_auth_parameters_from_config(config, async_client=True)

    assert client_auth_params["sasl_mechanism"] == "OAUTHBEARER"
    assert await client_auth_params["sasl_oauth_token_provider"].token() == "TOKEN"
