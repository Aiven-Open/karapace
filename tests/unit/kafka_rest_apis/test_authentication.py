"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import base64
import datetime
from collections.abc import Mapping
from http import HTTPStatus
from typing import Any

import jwt
import pytest

from karapace.core.container import KarapaceContainer
from karapace.kafka_rest_apis.authentication import (
    SimpleOauthTokenProvider,
    get_auth_config_from_header,
    get_expiration_time_from_header,
    get_kafka_client_auth_parameters_from_config,
)
from karapace.rapu import JSON_CONTENT_TYPE, HTTPResponse


def _assert_unauthorized_http_response(http_response: HTTPResponse) -> None:
    assert http_response.body == '{"message": "Unauthorized"}'
    assert http_response.status == HTTPStatus.UNAUTHORIZED
    assert http_response.headers["Content-Type"] == JSON_CONTENT_TYPE
    assert http_response.headers["WWW-Authenticate"] == 'Basic realm="Karapace REST Proxy"'


@pytest.mark.parametrize(
    "auth_header",
    (None, "Digest foo=bar"),
)
def test_get_auth_config_from_header_raises_unauthorized_on_invalid_header(
    karapace_container: KarapaceContainer, auth_header: str | None
) -> None:
    with pytest.raises(HTTPResponse) as exc_info:
        get_auth_config_from_header(auth_header, karapace_container.config())

    _assert_unauthorized_http_response(exc_info.value)


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
    karapace_container: KarapaceContainer,
    auth_header: str,
    config_override: Mapping[str, Any],
    expected_auth_config: Mapping[str, Any],
) -> None:
    config = karapace_container.config().set_config_defaults(new_config=config_override)
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


@pytest.mark.parametrize(
    "auth_header",
    (f"Bearer {jwt.encode({'exp': 1697013997}, 'secret')}XX", "Bearer NotAToken"),
)
def test_get_expiration_time_from_header_malformed_bearer_token_raises_unauthorized(auth_header: str) -> None:
    with pytest.raises(HTTPResponse) as exc_info:
        get_expiration_time_from_header(auth_header)

    _assert_unauthorized_http_response(exc_info.value)


def test_simple_oauth_token_provider_returns_configured_token_and_expiry() -> None:
    expiry_timestamp = 1697013997
    token = jwt.encode({"exp": expiry_timestamp}, "secret")
    token_provider = SimpleOauthTokenProvider(token)

    assert token_provider.token_with_expiry() == (token, expiry_timestamp)


def test_get_client_auth_parameters_from_config_sasl_plain(
    karapace_container: KarapaceContainer,
) -> None:
    config = karapace_container.config().set_config_defaults(
        new_config={"sasl_mechanism": "PLAIN", "sasl_plain_username": "username", "sasl_plain_password": "password"},
    )

    client_auth_params = get_kafka_client_auth_parameters_from_config(config)

    assert client_auth_params == {
        "sasl_mechanism": "PLAIN",
        "sasl_plain_username": "username",
        "sasl_plain_password": "password",
    }


def test_get_client_auth_parameters_from_config_oauth(
    karapace_container: KarapaceContainer,
) -> None:
    expiry_timestamp = 1697013997
    token = jwt.encode({"exp": expiry_timestamp}, "secret")
    config = karapace_container.config().set_config_defaults(
        new_config={"sasl_mechanism": "OAUTHBEARER", "sasl_oauth_token": token}
    )

    client_auth_params = get_kafka_client_auth_parameters_from_config(config)

    assert client_auth_params["sasl_mechanism"] == "OAUTHBEARER"
    assert client_auth_params["sasl_oauth_token_provider"].token_with_expiry() == (token, expiry_timestamp)


def test_get_client_auth_parameters_from_config_oauth_prefers_dynamic_provider(
    karapace_container: KarapaceContainer,
) -> None:
    """A configured `sasl_oauth_token_provider_class` wins over a static token.

    The REST proxy's own Kafka clients (admin, producers, consumers) go through
    this function; before, it asserted on `sasl_oauth_token` and ignored the
    dynamic provider the rest of Karapace already used, so a deployment on e.g.
    AWS MSK IAM could run the schema registry but not the REST proxy.
    """

    class DynamicTokenProvider:
        def token_with_expiry(self, config: str | None = None) -> tuple[str, int | None]:
            return ("dynamic-token", 1697013997)

    config = karapace_container.config().set_config_defaults(
        new_config={"sasl_mechanism": "OAUTHBEARER", "sasl_oauth_token_provider_class": DynamicTokenProvider}
    )
    config.model_post_init(None)

    client_auth_params = get_kafka_client_auth_parameters_from_config(config)

    assert client_auth_params["sasl_mechanism"] == "OAUTHBEARER"
    assert isinstance(client_auth_params["sasl_oauth_token_provider"], DynamicTokenProvider)
    assert client_auth_params["sasl_oauth_token_provider"].token_with_expiry() == ("dynamic-token", 1697013997)


def test_get_client_auth_parameters_from_config_oauth_requires_token_or_provider(
    karapace_container: KarapaceContainer,
) -> None:
    config = karapace_container.config().set_config_defaults(new_config={"sasl_mechanism": "OAUTHBEARER"})

    with pytest.raises(AssertionError, match="sasl_oauth_token_provider_class"):
        get_kafka_client_auth_parameters_from_config(config)
