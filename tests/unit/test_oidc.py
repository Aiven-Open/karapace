"""
Test config

Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from http import HTTPStatus
from unittest.mock import MagicMock, patch

import pytest
from jwt import InvalidTokenError
from karapace.api.oidc.middleware import OIDCMiddleware
from karapace.core.auth import AuthenticationError
from karapace.core.config import Config
from karapace.rapu import JSON_CONTENT_TYPE, HTTPResponse


def _assert_unauthorized_http_response(http_response: HTTPResponse) -> None:
    assert http_response.body == '{"message": "Unauthorized"}'
    assert http_response.status == HTTPStatus.UNAUTHORIZED
    assert http_response.headers["Content-Type"] == JSON_CONTENT_TYPE
    assert http_response.headers["WWW-Authenticate"] == 'Basic realm="Karapace REST Proxy"'


@dataclass
class DummyConfig:
    sasl_oauthbearer_jwks_endpoint_url: str | None
    sasl_oauthbearer_expected_issuer: str | None
    sasl_oauthbearer_expected_audience: str | None
    sasl_oauthbearer_sub_claim_name: str | None


valid_configs = [
    DummyConfig(
        sasl_oauthbearer_jwks_endpoint_url="http://oidcprovider/realms/testrealm/protocol/openid-connect/certs",
        sasl_oauthbearer_expected_issuer="https://oidcprovider.com",
        sasl_oauthbearer_expected_audience="accounts-audience",
        sasl_oauthbearer_sub_claim_name="sub",
    ),
    DummyConfig(
        sasl_oauthbearer_jwks_endpoint_url=None,
        sasl_oauthbearer_expected_issuer=None,
        sasl_oauthbearer_expected_audience=None,
        sasl_oauthbearer_sub_claim_name=None,
    ),
]

invalid_configs = [
    DummyConfig(
        sasl_oauthbearer_jwks_endpoint_url="http://oidcprovider/realms/testrealm/protocol/openid-connect/certs",
        sasl_oauthbearer_expected_issuer=None,
        sasl_oauthbearer_expected_audience="accounts-audience",
        sasl_oauthbearer_sub_claim_name="sub",
    ),
    DummyConfig(
        sasl_oauthbearer_jwks_endpoint_url="http://oidcprovider/realms/testrealm/protocol/openid-connect/certs",
        sasl_oauthbearer_expected_issuer=None,
        sasl_oauthbearer_expected_audience=None,
        sasl_oauthbearer_sub_claim_name="sub",
    ),
]


@pytest.fixture(scope="module", params=valid_configs)
def dummy_config(request):
    return request.param


@pytest.mark.parametrize("dummy_config", valid_configs, ids=["valid_full", "no_oidc"], indirect=True)
@pytest.mark.parametrize(
    ("auth_header", "mock_payload", "expected_expiration"),
    (
        # Token with a valid 'exp'
        (
            "Bearer valid.jwt.token",
            {"exp": int(datetime.datetime(2023, 10, 12, 12, 0, 0, tzinfo=datetime.timezone.utc).timestamp())},
            datetime.datetime(2023, 10, 12, 12, 0, 0, tzinfo=datetime.timezone.utc),
        ),
        # Token without 'exp'
        ("Bearer noexp.jwt.token", {}, None),
        # Token with 'exp' set to zero (Unix epoch start)
        ("Bearer zeroexp.jwt.token", {"exp": 0}, datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)),
        # Token with a far future expiration
        (
            "Bearer futureexp.jwt.token",
            {"exp": int(datetime.datetime(2100, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc).timestamp())},
            datetime.datetime(2100, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        ),
        # Token with 'exp' in the past (expired token)
        (
            "Bearer expired.jwt.token",
            {"exp": int(datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc).timestamp())},
            datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        ),
    ),
)
@patch("karapace.api.oidc.middleware.PyJWKClient")
@patch("karapace.api.oidc.middleware.jwt.decode")
def test_validate_token_valid_configs(
    mock_jwt_decode, mock_pyjwks_client, auth_header, mock_payload, expected_expiration, dummy_config
):
    mock_client_instance = MagicMock()
    mock_pyjwks_client.return_value = mock_client_instance
    mock_client_instance.get_signing_key_from_jwt.return_value.key = "fake-public-key"

    oidc_middleware = OIDCMiddleware(app=MagicMock(), config=dummy_config)
    mock_jwt_decode.return_value = mock_payload

    token = auth_header.split(" ", 1)[1]
    payload = oidc_middleware.validate_jwt(token)

    if dummy_config.sasl_oauthbearer_jwks_endpoint_url:
        # JWKS URL is present, validate normally
        if expected_expiration is not None:
            assert payload.get("exp") == int(expected_expiration.timestamp())
        else:
            assert "exp" not in payload or payload.get("exp") is None
    else:
        # No JWKS URL, validation is skipped, payload is empty dict
        assert payload == {}


@pytest.mark.parametrize("dummy_config", invalid_configs)
def test_oidc_middleware_raises_on_incomplete_config(dummy_config):
    config = Config(
        sasl_oauthbearer_jwks_endpoint_url=dummy_config.sasl_oauthbearer_jwks_endpoint_url,
        sasl_oauthbearer_expected_issuer=dummy_config.sasl_oauthbearer_expected_issuer,
        sasl_oauthbearer_expected_audience=dummy_config.sasl_oauthbearer_expected_audience,
        sasl_oauthbearer_sub_claim_name=dummy_config.sasl_oauthbearer_sub_claim_name,
    )
    with pytest.raises(
        ValueError, match="OIDC config error: 'issuer' and 'audience' must be set if 'jwks_endpoint_url' is provided."
    ):
        OIDCMiddleware(app=MagicMock(), config=config)


@pytest.mark.parametrize("dummy_config", valid_configs, ids=["valid_full", "no_oidc"], indirect=True)
@patch("karapace.api.oidc.middleware.PyJWKClient")
@patch("karapace.api.oidc.middleware.jwt.decode")
def test_validate_token_invalid_token(mock_jwt_decode, mock_pyjwks_client, dummy_config):
    # Setup mock PyJWKClient instance if JWKS URL is present, else None
    if dummy_config.sasl_oauthbearer_jwks_endpoint_url:
        mock_client_instance = MagicMock()
        mock_pyjwks_client.return_value = mock_client_instance
        mock_client_instance.get_signing_key_from_jwt.return_value.key = "fake-public-key"
    else:
        # If no JWKS URL, PyJWKClient should not be initialized, patch returns None or raise if used
        mock_pyjwks_client.return_value = None

    # Setup jwt.decode to raise InvalidTokenError
    mock_jwt_decode.side_effect = InvalidTokenError("Invalid token")

    oidc_middleware = OIDCMiddleware(app=MagicMock(), config=dummy_config)

    invalid_token = "invalid.jwt.token"

    if dummy_config.sasl_oauthbearer_jwks_endpoint_url:
        # Expect AuthenticationError to be raised on invalid token when validation is enabled
        with pytest.raises(AuthenticationError) as exc_info:
            oidc_middleware.validate_jwt(invalid_token)
        assert "Invalid OIDC token" in str(exc_info.value)
    else:
        # When JWKS URL is missing, validate_jwt returns empty dict instead of raising
        payload = oidc_middleware.validate_jwt(invalid_token)
        assert payload == {}
