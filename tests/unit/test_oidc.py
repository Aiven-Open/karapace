"""
Test config

Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import base64
import datetime
import logging
from dataclasses import dataclass, field
from http import HTTPStatus
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException
from jwt import ExpiredSignatureError, InvalidTokenError
from karapace.api.oidc.middleware import OIDCMiddleware, TokenExpiredError
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
    sasl_oauthbearer_authorization_enabled: bool
    sasl_oauthbearer_authentication_enabled: bool = False
    sasl_oauthbearer_roles_claim_path: str | None = None
    sasl_oauthbearer_method_roles: dict[str, list[str]] = field(
        default_factory=lambda: {"GET": [], "POST": [], "PUT": [], "DELETE": []}
    )
    sasl_oauthbearer_leeway_seconds: int = 30
    sasl_oauthbearer_require_access_token_typ: bool = False
    sasl_oauthbearer_allow_insecure_jwks = False


valid_configs = [
    DummyConfig(
        sasl_oauthbearer_jwks_endpoint_url="https://oidcprovider/realms/testrealm/protocol/openid-connect/certs",
        sasl_oauthbearer_expected_issuer="https://oidcprovider.com",
        sasl_oauthbearer_expected_audience="accounts-audience",
        sasl_oauthbearer_sub_claim_name="sub",
        sasl_oauthbearer_authorization_enabled=False,
        sasl_oauthbearer_roles_claim_path=None,
        sasl_oauthbearer_method_roles={"GET": [], "POST": [], "PUT": [], "DELETE": []},
    ),
    DummyConfig(
        sasl_oauthbearer_jwks_endpoint_url=None,
        sasl_oauthbearer_expected_issuer=None,
        sasl_oauthbearer_expected_audience=None,
        sasl_oauthbearer_sub_claim_name=None,
        sasl_oauthbearer_authorization_enabled=False,
        sasl_oauthbearer_roles_claim_path=None,
        sasl_oauthbearer_method_roles={"GET": [], "POST": [], "PUT": [], "DELETE": []},
    ),
]

invalid_configs = [
    DummyConfig(
        sasl_oauthbearer_jwks_endpoint_url="https://oidcprovider/realms/testrealm/protocol/openid-connect/certs",
        sasl_oauthbearer_expected_issuer=None,
        sasl_oauthbearer_expected_audience="accounts-audience",
        sasl_oauthbearer_sub_claim_name="sub",
        sasl_oauthbearer_authorization_enabled=False,
        sasl_oauthbearer_roles_claim_path=None,
        sasl_oauthbearer_method_roles={"GET": [], "POST": [], "PUT": [], "DELETE": []},
    ),
    DummyConfig(
        sasl_oauthbearer_jwks_endpoint_url="https://oidcprovider/realms/testrealm/protocol/openid-connect/certs",
        sasl_oauthbearer_expected_issuer=None,
        sasl_oauthbearer_expected_audience=None,
        sasl_oauthbearer_sub_claim_name="sub",
        sasl_oauthbearer_authorization_enabled=False,
        sasl_oauthbearer_roles_claim_path=None,
        sasl_oauthbearer_method_roles={"GET": [], "POST": [], "PUT": [], "DELETE": []},
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

    if dummy_config.sasl_oauthbearer_jwks_endpoint_url:
        payload = oidc_middleware.validate_jwt(token)
        if expected_expiration is not None:
            assert payload.get("exp") == int(expected_expiration.timestamp())
        else:
            assert "exp" not in payload or payload.get("exp") is None
    else:
        # No JWKS URL: validate_jwt fails closed instead of returning an empty payload.
        with pytest.raises(AuthenticationError, match="OIDC not configured"):
            oidc_middleware.validate_jwt(token)


@pytest.mark.parametrize("dummy_config", invalid_configs)
def test_oidc_middleware_raises_on_incomplete_config(dummy_config):
    config = Config(
        sasl_oauthbearer_jwks_endpoint_url=dummy_config.sasl_oauthbearer_jwks_endpoint_url,
        sasl_oauthbearer_expected_issuer=dummy_config.sasl_oauthbearer_expected_issuer,
        sasl_oauthbearer_expected_audience=dummy_config.sasl_oauthbearer_expected_audience,
        sasl_oauthbearer_sub_claim_name=dummy_config.sasl_oauthbearer_sub_claim_name,
        sasl_oauthbearer_authorization_enabled=dummy_config.sasl_oauthbearer_authorization_enabled,
    )
    with pytest.raises(
        ValueError, match="OIDC config error: 'issuer' and 'audience' must be set if 'jwks_endpoint_url' is provided."
    ):
        OIDCMiddleware(app=MagicMock(), config=config)


def test_oidc_middleware_rejects_http_jwks_url_by_default():
    config = Config(
        sasl_oauthbearer_jwks_endpoint_url="http://idp/realms/r/protocol/openid-connect/certs",
        sasl_oauthbearer_expected_issuer="http://idp/realms/r",
        sasl_oauthbearer_expected_audience="aud",
    )
    with pytest.raises(ValueError, match="https://"):
        OIDCMiddleware(app=MagicMock(), config=config)


@patch("karapace.api.oidc.middleware.PyJWKClient")
def test_oidc_middleware_allows_http_jwks_when_override_set(mock_pyjwks_client):
    mock_pyjwks_client.return_value = MagicMock()
    config = Config(
        sasl_oauthbearer_jwks_endpoint_url="http://idp/realms/r/protocol/openid-connect/certs",
        sasl_oauthbearer_expected_issuer="http://idp/realms/r",
        sasl_oauthbearer_expected_audience="aud",
        sasl_oauthbearer_allow_insecure_jwks=True,
    )
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
        # When JWKS URL is missing, validate_jwt fails closed.
        with pytest.raises(AuthenticationError, match="OIDC not configured"):
            oidc_middleware.validate_jwt(invalid_token)


@pytest.mark.parametrize(
    "payload,path,expected_roles",
    [
        ({"realm_access": {"roles": ["admin", "user"]}}, "realm_access.roles", ["admin", "user"]),
        ({"realm_access": {"roles": []}}, "realm_access.roles", []),
        ({"realm_access": {}}, "realm_access.roles", []),
        ({}, "realm_access.roles", []),
        ({"custom": {"nested": {"roles": ["reader"]}}}, "custom.nested.roles", ["reader"]),
        ({"custom": {"nested": {"roles": "notalist"}}}, "custom.nested.roles", []),
    ],
)
def test_get_roles_from_claim_path(payload, path, expected_roles):
    roles = OIDCMiddleware.get_roles_from_claim_path(payload, path)
    assert roles == expected_roles


# Padding for the other three required methods so the constructor's completeness
# check passes. Each row below only cares about ONE method; the others are inert.
_FULL_METHOD_ROLES = {"GET": ["admin"], "POST": ["admin"], "PUT": ["admin"], "DELETE": ["admin"]}


@pytest.mark.parametrize(
    "roles,method,method_roles,expect_error",
    [
        (["admin"], "GET", {**_FULL_METHOD_ROLES, "GET": ["admin"]}, False),
        (["user"], "POST", {**_FULL_METHOD_ROLES, "POST": ["admin", "user"]}, False),
        (["guest"], "DELETE", {**_FULL_METHOD_ROLES, "DELETE": ["admin"]}, True),
        ([], "GET", {**_FULL_METHOD_ROLES, "GET": ["admin"]}, True),
        # roles required but none configured for the method under test.
        (["reader"], "GET", {**_FULL_METHOD_ROLES, "GET": []}, True),
        # unsupported method (PATCH) -> get(METHOD, []) -> empty allow-list -> 403.
        (["admin"], "PATCH", _FULL_METHOD_ROLES, True),
    ],
)
def test_authorize_request_roles(monkeypatch, roles, method, method_roles, expect_error):
    config = DummyConfig(
        sasl_oauthbearer_jwks_endpoint_url="https://fake",
        sasl_oauthbearer_expected_issuer="issuer",
        sasl_oauthbearer_expected_audience="aud",
        sasl_oauthbearer_sub_claim_name="sub",
        sasl_oauthbearer_authorization_enabled=True,
        sasl_oauthbearer_roles_claim_path="realm_access.roles",
        sasl_oauthbearer_method_roles=method_roles,
    )
    middleware = OIDCMiddleware(app=MagicMock(), config=config)

    payload = {"realm_access": {"roles": roles}}

    if expect_error:
        with pytest.raises(HTTPException) as exc_info:
            middleware.authorize_request(payload, method)
        assert exc_info.value.status_code == 403
    else:
        middleware.authorize_request(payload, method)  # should not raise


# ---------------------------------------------------------------------------
# Tests for the authN / authZ flag split (sasl_oauthbearer_authentication_enabled
# is independent from sasl_oauthbearer_authorization_enabled).
# ---------------------------------------------------------------------------


def _oidc_config(**overrides) -> Config:
    """Build a Config with sane OIDC defaults plus any overrides for the test."""
    base: dict = {
        "sasl_oauthbearer_jwks_endpoint_url": "https://oidcprovider/realms/testrealm/protocol/openid-connect/certs",
        "sasl_oauthbearer_expected_issuer": "https://oidcprovider.com",
        "sasl_oauthbearer_expected_audience": "accounts-audience",
    }
    base.update(overrides)
    return Config(**base)


def test_config_bc_shim_authz_implies_authn(caplog):
    """Setting only authorization_enabled=True must auto-enable authentication_enabled with a warning."""
    with caplog.at_level(logging.WARNING):
        config = _oidc_config(sasl_oauthbearer_authorization_enabled=True)
    assert config.sasl_oauthbearer_authentication_enabled is True
    assert config.sasl_oauthbearer_authorization_enabled is True
    assert any("deprecated" in rec.message for rec in caplog.records)


def test_config_authn_only_does_not_warn(caplog):
    """Enabling only authN must not trigger the BC deprecation warning."""
    with caplog.at_level(logging.WARNING):
        config = _oidc_config(sasl_oauthbearer_authentication_enabled=True)
    assert config.sasl_oauthbearer_authentication_enabled is True
    assert config.sasl_oauthbearer_authorization_enabled is False
    assert not any("deprecated" in rec.message for rec in caplog.records)


def test_config_both_enabled_no_warning(caplog):
    with caplog.at_level(logging.WARNING):
        config = _oidc_config(
            sasl_oauthbearer_authentication_enabled=True,
            sasl_oauthbearer_authorization_enabled=True,
        )
    assert config.sasl_oauthbearer_authentication_enabled is True
    assert config.sasl_oauthbearer_authorization_enabled is True
    assert not any("deprecated" in rec.message for rec in caplog.records)


def test_config_both_disabled_default():
    config = Config()
    assert config.sasl_oauthbearer_authentication_enabled is False
    assert config.sasl_oauthbearer_authorization_enabled is False


# ---------------------------------------------------------------------------
# HTTP middleware behavior: authN-only vs authN+authZ vs disabled.
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_prometheus_registry():
    """setup_middlewares registers prometheus collectors into a global REGISTRY.
    Re-running it across tests in this module collides — unregister between tests.
    """
    from prometheus_client import REGISTRY

    yield
    for collector in list(REGISTRY._names_to_collectors.values()):
        try:
            REGISTRY.unregister(collector)
        except Exception:
            pass


def _build_app_with_middleware(
    monkeypatch, config: Config, *, validate_jwt_payload: dict | None = None, validate_jwt_raises: Exception | None = None
):
    """Build a minimal FastAPI app wired to setup_middlewares, with OIDCMiddleware patched."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from karapace.api import middlewares as middlewares_mod

    app = FastAPI()

    @app.get("/subjects")
    async def subjects():
        return []

    @app.get("/_health")
    async def health():
        return {"ok": True}

    def _fake_validate_jwt(self, token):
        if validate_jwt_raises is not None:
            raise validate_jwt_raises
        return validate_jwt_payload or {}

    def _fake_authorize(self, payload, method):
        # Re-use real role logic to verify the gate, but make it raise if enabled+roles missing.
        return True

    # Patch heavy bits of OIDCMiddleware: skip real JWKS client construction.
    monkeypatch.setattr(
        "karapace.api.oidc.middleware.PyJWKClient",
        lambda *a, **kw: MagicMock(),
    )
    monkeypatch.setattr(OIDCMiddleware, "validate_jwt", _fake_validate_jwt)
    monkeypatch.setattr(OIDCMiddleware, "authorize_request", _fake_authorize)

    middlewares_mod.setup_middlewares(app=app, config=config)
    return TestClient(app, raise_server_exceptions=False)


def test_middleware_disabled_allows_unauth_request(monkeypatch):
    config = Config()  # authN/authZ both False
    client = _build_app_with_middleware(monkeypatch, config, validate_jwt_payload={"sub": "u"})
    r = client.get("/subjects")
    assert r.status_code == 200


def test_middleware_authn_only_requires_bearer(monkeypatch):
    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True)
    client = _build_app_with_middleware(monkeypatch, config, validate_jwt_payload={"sub": "u"})
    r = client.get("/subjects")
    assert r.status_code == 401
    assert r.json()["reason"] == "Missing or invalid Authorization header"


def test_middleware_authn_only_valid_token_passes(monkeypatch):
    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True)
    client = _build_app_with_middleware(monkeypatch, config, validate_jwt_payload={"sub": "u"})
    r = client.get("/subjects", headers={"Authorization": "Bearer good.token"})
    assert r.status_code == 200


def test_middleware_authn_only_invalid_token_returns_401(monkeypatch):
    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True)
    client = _build_app_with_middleware(monkeypatch, config, validate_jwt_raises=AuthenticationError("bad"))
    r = client.get("/subjects", headers={"Authorization": "Bearer bad.token"})
    assert r.status_code == 401
    assert r.json()["reason"] == "Invalid token/payload"


def test_middleware_authn_only_skips_authorize_request(monkeypatch):
    """When only authN is enabled, authorize_request must NOT be invoked even with a valid token."""
    calls: list = []

    def _spy_authorize(self, payload, method):
        calls.append((payload, method))
        return True

    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True)
    monkeypatch.setattr(OIDCMiddleware, "authorize_request", _spy_authorize)
    client = _build_app_with_middleware(monkeypatch, config, validate_jwt_payload={"sub": "u"})
    # Re-patch authorize_request after _build_app_with_middleware (which also patches it).
    monkeypatch.setattr(OIDCMiddleware, "authorize_request", _spy_authorize)

    r = client.get("/subjects", headers={"Authorization": "Bearer good.token"})
    assert r.status_code == 200
    assert calls == []  # authZ never called


def test_middleware_authn_and_authz_calls_authorize(monkeypatch):
    """When both flags are on, authorize_request must be invoked with the validated payload."""
    calls: list = []

    def _spy_authorize(self, payload, method):
        calls.append((payload, method))
        return True

    config = _oidc_config(
        sasl_oauthbearer_authentication_enabled=True,
        sasl_oauthbearer_authorization_enabled=True,
        sasl_oauthbearer_roles_claim_path="realm_access.roles",
    )
    monkeypatch.setattr(OIDCMiddleware, "authorize_request", _spy_authorize)
    client = _build_app_with_middleware(monkeypatch, config, validate_jwt_payload={"sub": "u"})
    monkeypatch.setattr(OIDCMiddleware, "authorize_request", _spy_authorize)

    r = client.get("/subjects", headers={"Authorization": "Bearer good.token"})
    assert r.status_code == 200
    assert calls and calls[0][1] == "GET"


# ---------------------------------------------------------------------------
# RBAC through the full HTTP gate with the REAL authorize_request running
# (get_roles_from_claim_path + method_roles), not a spy. Complements the
# function-level authorize_request tests above.
# ---------------------------------------------------------------------------

_REAL_AUTHORIZE = OIDCMiddleware.authorize_request


def _authz_middleware_client(monkeypatch, payload: dict):
    """Build the middleware app with authZ on and the real authorize_request restored.
    method_roles: GET requires 'reader'. roles claim at realm_access.roles."""
    config = _oidc_config(
        sasl_oauthbearer_authentication_enabled=True,
        sasl_oauthbearer_authorization_enabled=True,
        sasl_oauthbearer_roles_claim_path="realm_access.roles",
        sasl_oauthbearer_method_roles={"GET": ["reader"], "POST": ["writer"], "PUT": ["writer"], "DELETE": ["writer"]},
    )
    client = _build_app_with_middleware(monkeypatch, config, validate_jwt_payload=payload)
    # _build_app_with_middleware stubs authorize_request; restore the genuine impl.
    monkeypatch.setattr(OIDCMiddleware, "authorize_request", _REAL_AUTHORIZE)
    return client


@pytest.mark.parametrize(
    ("roles_payload", "expected_status"),
    [
        ({"realm_access": {"roles": ["reader"]}}, 200),  # has required role
        ({"realm_access": {"roles": []}}, 403),  # empty roles list
        ({"realm_access": {"roles": ["someone-else"]}}, 403),  # wrong role
        ({"realm_access": {}}, 403),  # roles key missing at path
        ({}, 403),  # claim path absent entirely
    ],
)
def test_middleware_real_authorize_enforces_roles(monkeypatch, roles_payload, expected_status):
    client = _authz_middleware_client(monkeypatch, roles_payload)
    r = client.get("/subjects", headers={"Authorization": "Bearer good.token"})
    assert r.status_code == expected_status
    if expected_status == 403:
        assert r.json() == {"error": "Authorization error", "reason": "Forbidden"}


def test_middleware_real_authorize_denial_body_matches_misconfig(monkeypatch):
    """Anti-oracle: a real role-mismatch 403 must be byte-identical to the
    roles_claim_path-unset misconfig 403, so the response can't distinguish them."""
    # Role-mismatch denial through the gate.
    mismatch_client = _authz_middleware_client(monkeypatch, {"realm_access": {"roles": ["nope"]}})
    mismatch = mismatch_client.get("/subjects", headers={"Authorization": "Bearer good.token"})
    assert mismatch.status_code == 403

    # Misconfig branch: roles_claim_path is None at authorize time -> same 403 body.
    ns = SimpleNamespace(
        authorization_enabled=True,
        sasl_oauthbearer_method_roles={"GET": ["reader"], "POST": ["w"], "PUT": ["w"], "DELETE": ["w"]},
        sasl_oauthbearer_roles_claim_path=None,
        get_roles_from_claim_path=OIDCMiddleware.get_roles_from_claim_path,
    )
    with pytest.raises(HTTPException) as exc_info:
        OIDCMiddleware.authorize_request(ns, {"realm_access": {"roles": ["reader"]}}, request_method="GET")
    assert exc_info.value.status_code == 403
    # The middleware serializes HTTPException.detail into reason; both must render identically.
    assert mismatch.json() == {"error": "Authorization error", "reason": exc_info.value.detail}


def test_middleware_authn_and_authz_returns_403_on_role_failure(monkeypatch):
    def _deny(self, payload, method):
        raise HTTPException(status_code=403, detail="Insufficient roles")

    config = _oidc_config(
        sasl_oauthbearer_authentication_enabled=True,
        sasl_oauthbearer_authorization_enabled=True,
        sasl_oauthbearer_roles_claim_path="realm_access.roles",
    )
    monkeypatch.setattr(OIDCMiddleware, "authorize_request", _deny)
    client = _build_app_with_middleware(monkeypatch, config, validate_jwt_payload={"sub": "u"})
    monkeypatch.setattr(OIDCMiddleware, "authorize_request", _deny)

    r = client.get("/subjects", headers={"Authorization": "Bearer good.token"})
    assert r.status_code == 403
    assert r.json()["reason"] == "Insufficient roles"


def test_middleware_skip_paths_bypass_auth(monkeypatch):
    """Paths in sasl_oauthbearer_skip_auth_paths must bypass the gate even when authN is enabled."""
    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True)
    client = _build_app_with_middleware(monkeypatch, config, validate_jwt_payload={"sub": "u"})
    r = client.get("/_health")  # no Authorization header
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# Authorization-header parsing branches in _authenticate_and_authorize.
# The scheme check is case- and space-sensitive; only the missing-header branch
# was previously covered.
# ---------------------------------------------------------------------------


# Encode at runtime so no literal ``Basic <base64>`` credential lands in source
# (a hardcoded one trips secret scanners).
_BASIC_HEADER = "Basic " + base64.b64encode(b"user:pass").decode()


@pytest.mark.parametrize(
    "auth_header",
    [
        _BASIC_HEADER,  # wrong scheme
        "bearer good.token",  # lowercase scheme — check is case-sensitive
        "Bearer",  # scheme only, no token/space
        "Bearer ",  # empty token
        "Bearer    ",  # whitespace-only token
    ],
)
def test_middleware_rejects_non_bearer_or_empty_token(monkeypatch, auth_header):
    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True)
    client = _build_app_with_middleware(monkeypatch, config, validate_jwt_payload={"sub": "u"})
    r = client.get("/subjects", headers={"Authorization": auth_header})
    assert r.status_code == 401
    assert r.json()["reason"] == "Missing or invalid Authorization header"


def test_middleware_accepts_bearer_with_valid_token(monkeypatch):
    """Control for the rejection cases above."""
    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True)
    client = _build_app_with_middleware(monkeypatch, config, validate_jwt_payload={"sub": "u"})
    r = client.get("/subjects", headers={"Authorization": "Bearer good.token"})
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# Expired token: ExpiredSignatureError must surface as TokenExpiredError from
# validate_jwt and translate to a 401 with reason "Token expired".
# ---------------------------------------------------------------------------


@patch("karapace.api.oidc.middleware.PyJWKClient")
@patch("karapace.api.oidc.middleware.jwt.decode")
def test_validate_jwt_expired_token_raises_token_expired(mock_jwt_decode, mock_pyjwks_client):
    mock_pyjwks_client.return_value = MagicMock()
    mock_pyjwks_client.return_value.get_signing_key_from_jwt.return_value.key = "fake-public-key"
    mock_jwt_decode.side_effect = ExpiredSignatureError("Signature has expired")

    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True)
    middleware = OIDCMiddleware(app=MagicMock(), config=config)

    with pytest.raises(TokenExpiredError) as exc_info:
        middleware.validate_jwt("expired.jwt.token")
    assert "expired" in str(exc_info.value).lower()
    # TokenExpiredError must remain a subclass of AuthenticationError so existing handlers still catch it.
    assert isinstance(exc_info.value, AuthenticationError)


def test_middleware_returns_401_token_expired_on_expired_token(monkeypatch):
    """An expired token must produce a 401 with a distinct 'Token expired' reason."""
    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True)
    client = _build_app_with_middleware(
        monkeypatch,
        config,
        validate_jwt_raises=TokenExpiredError("OIDC token expired"),
    )
    r = client.get("/subjects", headers={"Authorization": "Bearer expired.token"})
    assert r.status_code == 401
    assert r.json()["error"] == "Unauthorized"
    assert r.json()["reason"] == "Token expired"


def test_middleware_invalid_token_still_returns_invalid_reason(monkeypatch):
    """Generic AuthenticationError (non-expiry) must keep the original 'Invalid token/payload' reason."""
    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True)
    client = _build_app_with_middleware(
        monkeypatch,
        config,
        validate_jwt_raises=AuthenticationError("bad sig"),
    )
    r = client.get("/subjects", headers={"Authorization": "Bearer bogus.token"})
    assert r.status_code == 401
    assert r.json()["reason"] == "Invalid token/payload"


# ---------------------------------------------------------------------------
# Hardening: leeway, require sub, log reason, audience guard, typ:at+jwt.
# ---------------------------------------------------------------------------


@patch("karapace.api.oidc.middleware.PyJWKClient")
@patch("karapace.api.oidc.middleware.jwt.decode")
def test_validate_jwt_passes_leeway_and_require_sub_to_decode(mock_jwt_decode, mock_pyjwks_client):
    mock_pyjwks_client.return_value = MagicMock()
    mock_pyjwks_client.return_value.get_signing_key_from_jwt.return_value.key = "fake-public-key"
    mock_jwt_decode.return_value = {"sub": "u"}

    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True, sasl_oauthbearer_leeway_seconds=42)
    middleware = OIDCMiddleware(app=MagicMock(), config=config)
    middleware.validate_jwt("good.jwt.token")

    kwargs = mock_jwt_decode.call_args.kwargs
    assert kwargs["leeway"] == 42
    assert "sub" in kwargs["options"]["require"]


@patch("karapace.api.oidc.middleware.PyJWKClient")
@patch("karapace.api.oidc.middleware.jwt.decode")
def test_validate_jwt_require_uses_configured_sub_claim(mock_jwt_decode, mock_pyjwks_client):
    mock_pyjwks_client.return_value = MagicMock()
    mock_pyjwks_client.return_value.get_signing_key_from_jwt.return_value.key = "fake-public-key"
    mock_jwt_decode.return_value = {"user_id": "u"}

    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True, sasl_oauthbearer_sub_claim_name="user_id")
    middleware = OIDCMiddleware(app=MagicMock(), config=config)
    middleware.validate_jwt("good.jwt.token")

    require = mock_jwt_decode.call_args.kwargs["options"]["require"]
    assert "user_id" in require


@patch("karapace.api.oidc.middleware.PyJWKClient")
@patch("karapace.api.oidc.middleware.jwt.decode")
def test_validate_jwt_logs_reason_on_invalid_token(mock_jwt_decode, mock_pyjwks_client, caplog):
    mock_pyjwks_client.return_value = MagicMock()
    mock_pyjwks_client.return_value.get_signing_key_from_jwt.return_value.key = "fake-public-key"
    mock_jwt_decode.side_effect = InvalidTokenError("missing required claim sub")

    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True)
    middleware = OIDCMiddleware(app=MagicMock(), config=config)

    with caplog.at_level(logging.WARNING, logger="karapace.api.oidc.middleware"):
        with pytest.raises(AuthenticationError):
            middleware.validate_jwt("bad.jwt.token")

    assert any("missing required claim sub" in rec.message for rec in caplog.records)


@patch("karapace.api.oidc.middleware.PyJWKClient")
def test_validate_jwt_audience_missing_raises(mock_pyjwks_client):
    """Defense-in-depth: if audience is somehow None at decode time, fail closed."""
    mock_pyjwks_client.return_value = MagicMock()

    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True)
    middleware = OIDCMiddleware(app=MagicMock(), config=config)
    middleware.audience = None  # bypass __init__ guard

    with pytest.raises(AuthenticationError, match="audience missing"):
        middleware.validate_jwt("any.jwt.token")


@patch("karapace.api.oidc.middleware.PyJWKClient")
@patch("karapace.api.oidc.middleware.jwt.get_unverified_header")
@patch("karapace.api.oidc.middleware.jwt.decode")
def test_validate_jwt_at_jwt_typ_enforced_accepts(mock_jwt_decode, mock_unverified_header, mock_pyjwks_client):
    mock_pyjwks_client.return_value = MagicMock()
    mock_pyjwks_client.return_value.get_signing_key_from_jwt.return_value.key = "fake-public-key"
    mock_unverified_header.return_value = {"typ": "at+jwt"}
    mock_jwt_decode.return_value = {"sub": "u"}

    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True, sasl_oauthbearer_require_access_token_typ=True)
    middleware = OIDCMiddleware(app=MagicMock(), config=config)
    assert middleware.validate_jwt("good.jwt.token") == {"sub": "u"}


@patch("karapace.api.oidc.middleware.PyJWKClient")
@patch("karapace.api.oidc.middleware.jwt.get_unverified_header")
@patch("karapace.api.oidc.middleware.jwt.decode")
def test_validate_jwt_at_jwt_typ_enforced_rejects_id_token(mock_jwt_decode, mock_unverified_header, mock_pyjwks_client):
    mock_pyjwks_client.return_value = MagicMock()
    mock_pyjwks_client.return_value.get_signing_key_from_jwt.return_value.key = "fake-public-key"
    mock_unverified_header.return_value = {"typ": "JWT"}
    mock_jwt_decode.return_value = {"sub": "u"}

    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True, sasl_oauthbearer_require_access_token_typ=True)
    middleware = OIDCMiddleware(app=MagicMock(), config=config)
    with pytest.raises(AuthenticationError, match="Invalid OIDC token"):
        middleware.validate_jwt("idtoken.jwt.token")
    mock_jwt_decode.assert_not_called()


@patch("karapace.api.oidc.middleware.PyJWKClient")
@patch("karapace.api.oidc.middleware.jwt.get_unverified_header")
@patch("karapace.api.oidc.middleware.jwt.decode")
def test_validate_jwt_typ_check_skipped_when_flag_off(mock_jwt_decode, mock_unverified_header, mock_pyjwks_client):
    mock_pyjwks_client.return_value = MagicMock()
    mock_pyjwks_client.return_value.get_signing_key_from_jwt.return_value.key = "fake-public-key"
    mock_jwt_decode.return_value = {"sub": "u"}

    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True)
    middleware = OIDCMiddleware(app=MagicMock(), config=config)
    middleware.validate_jwt("good.jwt.token")
    mock_unverified_header.assert_not_called()


@patch("karapace.api.oidc.middleware.PyJWKClient")
def test_authorization_enabled_without_client_id_is_accepted(mock_pyjwks_client):
    """Authz needs only a static roles_claim_path — client_id is no longer required."""
    mock_pyjwks_client.return_value = MagicMock()
    config = _oidc_config(
        sasl_oauthbearer_authentication_enabled=True,
        sasl_oauthbearer_authorization_enabled=True,
        sasl_oauthbearer_roles_claim_path="realm_access.roles",
        sasl_oauthbearer_method_roles={"GET": ["reader"], "POST": ["admin"], "PUT": ["admin"], "DELETE": ["admin"]},
    )
    middleware = OIDCMiddleware(app=MagicMock(), config=config)
    payload = {"realm_access": {"roles": ["reader"]}}
    assert middleware.authorize_request(payload, "GET") is True


@patch("karapace.api.oidc.middleware.PyJWKClient")
@patch("karapace.api.oidc.middleware.jwt.decode")
def test_validate_jwt_default_leeway_is_zero(mock_jwt_decode, mock_pyjwks_client):
    """Default leeway is 0 — preserves prior strict behavior. Operators opt into skew tolerance."""
    mock_pyjwks_client.return_value = MagicMock()
    mock_pyjwks_client.return_value.get_signing_key_from_jwt.return_value.key = "fake-public-key"
    mock_jwt_decode.return_value = {"sub": "u"}

    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True)
    middleware = OIDCMiddleware(app=MagicMock(), config=config)
    middleware.validate_jwt("good.jwt.token")

    assert mock_jwt_decode.call_args.kwargs["leeway"] == 0


def test_config_rejects_negative_leeway():
    """Pydantic Field(ge=0) rejects negative values at config-parse time."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Config(sasl_oauthbearer_leeway_seconds=-1)


@pytest.mark.parametrize("typ_value", ["AT+JWT", "at+jwt", "application/at+jwt", "Application/AT+JWT"])
@patch("karapace.api.oidc.middleware.PyJWKClient")
@patch("karapace.api.oidc.middleware.jwt.get_unverified_header")
@patch("karapace.api.oidc.middleware.jwt.decode")
def test_validate_jwt_at_jwt_typ_accepts_case_insensitive_and_long_form(
    mock_jwt_decode, mock_unverified_header, mock_pyjwks_client, typ_value
):
    mock_pyjwks_client.return_value = MagicMock()
    mock_pyjwks_client.return_value.get_signing_key_from_jwt.return_value.key = "fake-public-key"
    mock_unverified_header.return_value = {"typ": typ_value}
    mock_jwt_decode.return_value = {"sub": "u"}

    config = _oidc_config(sasl_oauthbearer_authentication_enabled=True, sasl_oauthbearer_require_access_token_typ=True)
    middleware = OIDCMiddleware(app=MagicMock(), config=config)
    assert middleware.validate_jwt("good.jwt.token") == {"sub": "u"}


# ``sasl_oauthbearer_method_roles`` completeness — regression for the
# ``set(keys) - keys`` no-op that let incomplete configs silently 403 at runtime.


def _authz_config(method_roles: dict[str, list[str]]) -> Config:
    """Build a fully-valid OIDC authz config with the supplied ``method_roles``."""
    return _oidc_config(
        sasl_oauthbearer_authentication_enabled=True,
        sasl_oauthbearer_authorization_enabled=True,
        sasl_oauthbearer_roles_claim_path="resource_access.karapace-client.roles",
        sasl_oauthbearer_method_roles=method_roles,
    )


@patch("karapace.api.oidc.middleware.PyJWKClient")
def test_method_roles_complete_set_is_accepted(mock_pyjwks_client):
    """All four required methods present -> constructor succeeds."""
    mock_pyjwks_client.return_value = MagicMock()
    config = _authz_config({"GET": ["r"], "POST": ["w"], "PUT": ["w"], "DELETE": ["w"]})
    OIDCMiddleware(app=MagicMock(), config=config)


@pytest.mark.parametrize(
    ("method_roles", "expected_missing"),
    [
        # Single missing method — the original silent-failure case.
        ({"GET": ["r"], "PUT": ["w"], "DELETE": ["w"]}, "POST"),
        ({"GET": ["r"], "POST": ["w"], "PUT": ["w"]}, "DELETE"),
        ({"POST": ["w"], "PUT": ["w"], "DELETE": ["w"]}, "GET"),
        # Multiple missing — message must list them sorted for determinism.
        ({"GET": ["r"], "POST": ["w"]}, "DELETE, PUT"),
        # Empty dict — every required method is missing.
        ({}, "DELETE, GET, POST, PUT"),
    ],
)
@patch("karapace.api.oidc.middleware.PyJWKClient")
def test_method_roles_missing_required_method_raises(mock_pyjwks_client, method_roles, expected_missing):
    mock_pyjwks_client.return_value = MagicMock()
    config = _authz_config(method_roles)
    with pytest.raises(ValueError, match=f"method_roles is missing definitions for: {expected_missing}"):
        OIDCMiddleware(app=MagicMock(), config=config)


@patch("karapace.api.oidc.middleware.PyJWKClient")
def test_method_roles_extra_unknown_method_is_allowed(mock_pyjwks_client):
    """Extra methods beyond the required set are tolerated — only missing ones fail."""
    mock_pyjwks_client.return_value = MagicMock()
    config = _authz_config({"GET": ["r"], "POST": ["w"], "PUT": ["w"], "DELETE": ["w"], "PATCH": ["w"]})
    OIDCMiddleware(app=MagicMock(), config=config)


@patch("karapace.api.oidc.middleware.PyJWKClient")
def test_method_roles_validation_is_case_insensitive(mock_pyjwks_client):
    """Lowercase method keys are normalised before the completeness check."""
    mock_pyjwks_client.return_value = MagicMock()
    config = _authz_config({"get": ["r"], "post": ["w"], "put": ["w"], "delete": ["w"]})
    OIDCMiddleware(app=MagicMock(), config=config)


@patch("karapace.api.oidc.middleware.PyJWKClient")
def test_method_roles_not_validated_when_authorization_disabled(mock_pyjwks_client):
    """``method_roles`` is irrelevant when authz is off — even an empty dict is fine."""
    mock_pyjwks_client.return_value = MagicMock()
    config = _oidc_config(
        sasl_oauthbearer_authentication_enabled=True,
        sasl_oauthbearer_authorization_enabled=False,
        sasl_oauthbearer_method_roles={},
    )
    OIDCMiddleware(app=MagicMock(), config=config)


@patch("karapace.api.oidc.middleware.PyJWKClient")
def test_constructor_refuses_misconfigured_method_roles(mock_pyjwks_client):
    """The check refuses a config missing ``POST`` upfront at startup."""
    mock_pyjwks_client.return_value = MagicMock()
    bad_config = _authz_config({"GET": ["r"], "PUT": ["w"], "DELETE": ["w"]})
    with pytest.raises(ValueError, match="method_roles is missing definitions for: POST"):
        OIDCMiddleware(app=MagicMock(), config=bad_config)


def test_authorize_request_silently_denies_when_method_unmapped():
    """Documents the runtime failure mode the constructor check exists to prevent:
    an unmapped method falls back to ``allowed_roles=[]`` and unconditionally 403s,
    regardless of which roles the token carries.
    """
    ns = SimpleNamespace(
        authorization_enabled=True,
        sasl_oauthbearer_method_roles={"GET": ["r"], "PUT": ["w"], "DELETE": ["w"]},
        sasl_oauthbearer_roles_claim_path="resource_access.karapace-client.roles",
        client_id="karapace-client",
        get_roles_from_claim_path=OIDCMiddleware.get_roles_from_claim_path,
    )
    payload = {"resource_access": {"karapace-client": {"roles": ["w"]}}}

    with pytest.raises(HTTPException) as exc_info:
        OIDCMiddleware.authorize_request(ns, payload, request_method="POST")
    assert exc_info.value.status_code == 403


# ---------------------------------------------------------------------------
# Real-key JWT validation. The tests above mock jwt.decode, so they only prove
# we pass the right arguments — PyJWT's own aud/iss/exp/nbf/algorithm checks are
# never exercised. Here we sign real RS256 tokens with an in-test RSA keypair and
# stub ONLY the JWKS client to hand back the matching public key, so validate_jwt
# runs the genuine decode path.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def rsa_keypair():
    """A single RSA keypair for the module. PyJWT accepts the cryptography key
    objects directly, so no PEM round-trip is needed."""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


def _make_token(signing_key, claims: dict, *, headers: dict | None = None, algorithm: str = "RS256") -> str:
    return jwt.encode(claims, signing_key, algorithm=algorithm, headers=headers)


def _base_claims(**overrides) -> dict:
    """Valid claims for the default _oidc_config (issuer/audience) with a live exp/iat."""
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    claims = {
        "sub": "user-1",
        "iss": "https://oidcprovider.com",
        "aud": "accounts-audience",
        "exp": int((now + datetime.timedelta(hours=1)).timestamp()),
        "iat": int(now.timestamp()),
    }
    claims.update(overrides)
    return {k: v for k, v in claims.items() if v is not _OMIT}


_OMIT = object()  # sentinel: pass claim=_OMIT to drop it entirely


@pytest.fixture
def build_real_middleware(rsa_keypair):
    """Build an OIDCMiddleware whose JWKS client returns the in-test public key,
    so validate_jwt runs the real PyJWT decode against real signatures."""
    _, public_key = rsa_keypair

    def _build(**config_overrides) -> OIDCMiddleware:
        with patch("karapace.api.oidc.middleware.PyJWKClient") as mock_client:
            instance = MagicMock()
            instance.get_signing_key_from_jwt.return_value.key = public_key
            mock_client.return_value = instance
            config = _oidc_config(sasl_oauthbearer_authentication_enabled=True, **config_overrides)
            # _jwks_client is captured on the instance during __init__, so it survives the patch exit.
            return OIDCMiddleware(app=MagicMock(), config=config)

    return _build


def test_real_jwt_valid_token_decodes(rsa_keypair, build_real_middleware):
    private_key, _ = rsa_keypair
    middleware = build_real_middleware()
    payload = middleware.validate_jwt(_make_token(private_key, _base_claims()))
    assert payload["sub"] == "user-1"


# --- audience ---


def test_real_jwt_wrong_audience_rejected(rsa_keypair, build_real_middleware):
    private_key, _ = rsa_keypair
    middleware = build_real_middleware()
    token = _make_token(private_key, _base_claims(aud="some-other-audience"))
    with pytest.raises(AuthenticationError, match="Invalid OIDC token"):
        middleware.validate_jwt(token)


def test_real_jwt_missing_audience_rejected(rsa_keypair, build_real_middleware):
    private_key, _ = rsa_keypair
    middleware = build_real_middleware()
    token = _make_token(private_key, _base_claims(aud=_OMIT))
    with pytest.raises(AuthenticationError, match="Invalid OIDC token"):
        middleware.validate_jwt(token)


def test_real_jwt_one_of_multiple_configured_audiences_accepted(rsa_keypair, build_real_middleware):
    """Comma-separated expected audiences: a token matching any one is accepted."""
    private_key, _ = rsa_keypair
    middleware = build_real_middleware(sasl_oauthbearer_expected_audience="aud-one,aud-two")
    token = _make_token(private_key, _base_claims(aud="aud-two"))
    assert middleware.validate_jwt(token)["aud"] == "aud-two"


# --- issuer ---


def test_real_jwt_wrong_issuer_rejected(rsa_keypair, build_real_middleware):
    private_key, _ = rsa_keypair
    middleware = build_real_middleware()
    token = _make_token(private_key, _base_claims(iss="https://evil.example.com"))
    with pytest.raises(AuthenticationError, match="Invalid OIDC token"):
        middleware.validate_jwt(token)


def test_real_jwt_missing_issuer_rejected(rsa_keypair, build_real_middleware):
    private_key, _ = rsa_keypair
    middleware = build_real_middleware()
    token = _make_token(private_key, _base_claims(iss=_OMIT))
    with pytest.raises(AuthenticationError, match="Invalid OIDC token"):
        middleware.validate_jwt(token)


# --- expiry / leeway ---


def test_real_jwt_expired_raises_token_expired(rsa_keypair, build_real_middleware):
    private_key, _ = rsa_keypair
    middleware = build_real_middleware()
    past = int((datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(hours=1)).timestamp())
    with pytest.raises(TokenExpiredError):
        middleware.validate_jwt(_make_token(private_key, _base_claims(exp=past)))


def test_real_jwt_expired_within_leeway_accepted(rsa_keypair, build_real_middleware):
    private_key, _ = rsa_keypair
    middleware = build_real_middleware(sasl_oauthbearer_leeway_seconds=60)
    just_expired = int((datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(seconds=10)).timestamp())
    assert middleware.validate_jwt(_make_token(private_key, _base_claims(exp=just_expired)))


def test_real_jwt_missing_exp_rejected(rsa_keypair, build_real_middleware):
    private_key, _ = rsa_keypair
    middleware = build_real_middleware()
    with pytest.raises(AuthenticationError, match="Invalid OIDC token"):
        middleware.validate_jwt(_make_token(private_key, _base_claims(exp=_OMIT)))


# --- not-before ---


def test_real_jwt_future_nbf_rejected(rsa_keypair, build_real_middleware):
    private_key, _ = rsa_keypair
    middleware = build_real_middleware()
    future = int((datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(hours=1)).timestamp())
    with pytest.raises(AuthenticationError, match="Invalid OIDC token"):
        middleware.validate_jwt(_make_token(private_key, _base_claims(nbf=future)))


def test_real_jwt_nbf_within_leeway_accepted(rsa_keypair, build_real_middleware):
    private_key, _ = rsa_keypair
    middleware = build_real_middleware(sasl_oauthbearer_leeway_seconds=60)
    soon = int((datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=10)).timestamp())
    assert middleware.validate_jwt(_make_token(private_key, _base_claims(nbf=soon)))


# --- subject / configured claim name ---


def test_real_jwt_missing_sub_rejected(rsa_keypair, build_real_middleware):
    private_key, _ = rsa_keypair
    middleware = build_real_middleware()
    with pytest.raises(AuthenticationError, match="Invalid OIDC token"):
        middleware.validate_jwt(_make_token(private_key, _base_claims(sub=_OMIT)))


def test_real_jwt_alternate_sub_claim_required(rsa_keypair, build_real_middleware):
    private_key, _ = rsa_keypair
    middleware = build_real_middleware(sasl_oauthbearer_sub_claim_name="user_id")
    # Present: accepted.
    ok = _make_token(private_key, _base_claims(sub=_OMIT, user_id="u-42"))
    assert middleware.validate_jwt(ok)["user_id"] == "u-42"
    # Absent: the configured claim is in the require list, so decode rejects.
    with pytest.raises(AuthenticationError, match="Invalid OIDC token"):
        middleware.validate_jwt(_make_token(private_key, _base_claims(sub=_OMIT)))


# --- signing algorithm allow-list (security critical) ---


def test_real_jwt_hs256_symmetric_token_rejected(build_real_middleware):
    """A token signed with a symmetric HS256 secret must be rejected: the allow-list is RSA-only,
    so an attacker cannot smuggle the public key in as an HMAC secret."""
    middleware = build_real_middleware()
    token = _make_token("attacker-known-secret", _base_claims(), algorithm="HS256")
    with pytest.raises(AuthenticationError, match="Invalid OIDC token"):
        middleware.validate_jwt(token)


def test_real_jwt_alg_none_unsigned_token_rejected(build_real_middleware):
    """An unsigned ('alg': 'none') token must be rejected."""
    middleware = build_real_middleware()
    token = _make_token(None, _base_claims(), algorithm="none")
    with pytest.raises(AuthenticationError, match="Invalid OIDC token"):
        middleware.validate_jwt(token)


# --- typ header enforcement (real signature, real header) ---


def test_real_jwt_typ_at_jwt_accepted_when_required(rsa_keypair, build_real_middleware):
    private_key, _ = rsa_keypair
    middleware = build_real_middleware(sasl_oauthbearer_require_access_token_typ=True)
    token = _make_token(private_key, _base_claims(), headers={"typ": "at+jwt"})
    assert middleware.validate_jwt(token)["sub"] == "user-1"


def test_real_jwt_id_token_typ_rejected_when_required(rsa_keypair, build_real_middleware):
    private_key, _ = rsa_keypair
    middleware = build_real_middleware(sasl_oauthbearer_require_access_token_typ=True)
    token = _make_token(private_key, _base_claims(), headers={"typ": "JWT"})
    with pytest.raises(AuthenticationError, match="Invalid OIDC token"):
        middleware.validate_jwt(token)
