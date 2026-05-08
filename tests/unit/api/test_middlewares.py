"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Iterator
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient
from karapace.api.middlewares import setup_middlewares
from karapace.core.auth import AuthenticationError
from karapace.core.config import Config
from prometheus_client import REGISTRY
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _clean_prometheus_registry() -> Iterator[None]:
    yield
    for collector in set(REGISTRY._names_to_collectors.values()):
        try:
            REGISTRY.unregister(collector)
        except Exception:
            pass


def _build_app(config: Config) -> FastAPI:
    app = FastAPI()

    @app.get("/subjects")
    async def subjects(request: Request) -> dict:
        request.state.schema_response_content_type = "application/vnd.schemaregistry.v1+json"
        return {"ok": True}

    @app.get("/_health")
    async def health() -> dict:
        return {"status": "ok"}

    setup_middlewares(app, config)
    return app


def _client(config: Config) -> TestClient:
    return TestClient(_build_app(config), raise_server_exceptions=False)


# ---- Skip paths (docs, health/metrics) ----


def test_docs_path_bypasses_auth() -> None:
    config = Config(sasl_oauthbearer_authorization_enabled=True)

    response = _client(config).get("/docs")

    # FastAPI serves docs HTML; what matters is we didn't get 401 from the middleware.
    assert response.status_code != 401


def test_skip_auth_path_bypasses_bearer_check() -> None:
    config = Config(
        sasl_oauthbearer_authorization_enabled=True,
        sasl_oauthbearer_skip_auth_paths=["/_health", "/metrics"],
    )

    response = _client(config).get("/_health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# ---- Authorization disabled: middleware shouldn't require token ----


def test_auth_disabled_allows_requests_without_token() -> None:
    config = Config(sasl_oauthbearer_authorization_enabled=False)

    response = _client(config).get("/subjects")

    assert response.status_code == 200


# ---- Authorization enabled: missing / malformed Bearer header ----


def test_missing_authorization_header_returns_401() -> None:
    config = Config(sasl_oauthbearer_authorization_enabled=True)

    response = _client(config).get("/subjects")

    assert response.status_code == 401
    body = response.json()
    assert body["error"] == "Unauthorized"
    assert "Missing or invalid" in body["reason"]


def test_non_bearer_authorization_returns_401() -> None:
    config = Config(sasl_oauthbearer_authorization_enabled=True)

    response = _client(config).get("/subjects", headers={"Authorization": "Basic abc"})

    assert response.status_code == 401


# ---- Authorization enabled: JWT validation outcomes ----


def test_valid_bearer_token_passes_and_sets_content_type() -> None:
    config = Config(sasl_oauthbearer_authorization_enabled=True)

    with (
        patch("karapace.api.middlewares.OIDCMiddleware.validate_jwt", return_value={"sub": "u1"}),
        patch("karapace.api.middlewares.OIDCMiddleware.authorize_request", return_value=True),
    ):
        response = _client(config).get("/subjects", headers={"Authorization": "Bearer good-token"})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/vnd.schemaregistry.v1+json")


def test_invalid_jwt_returns_401() -> None:
    config = Config(sasl_oauthbearer_authorization_enabled=True)

    with patch(
        "karapace.api.middlewares.OIDCMiddleware.validate_jwt",
        side_effect=AuthenticationError("bad token"),
    ):
        response = _client(config).get("/subjects", headers={"Authorization": "Bearer bad-token"})

    assert response.status_code == 401
    assert response.json()["reason"] == "Invalid token/payload"


def test_authorization_failure_returns_role_error() -> None:
    config = Config(sasl_oauthbearer_authorization_enabled=True)

    with (
        patch("karapace.api.middlewares.OIDCMiddleware.validate_jwt", return_value={"sub": "u1"}),
        patch(
            "karapace.api.middlewares.OIDCMiddleware.authorize_request",
            side_effect=HTTPException(status_code=403, detail="Insufficient roles"),
        ),
    ):
        response = _client(config).get("/subjects", headers={"Authorization": "Bearer good-token"})

    assert response.status_code == 403
    body = response.json()
    assert body["error"] == "Authorization error"
    assert body["reason"] == "Insufficient roles"


# ---- Content-Type override when no request.state attr is set ----


def test_content_type_override_not_set_when_absent() -> None:
    config = Config()
    app = FastAPI()

    @app.get("/no-override")
    async def no_override() -> dict:
        return {"hello": "world"}

    setup_middlewares(app, config)
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/no-override")

    assert response.status_code == 200
    # Default JSON content type, not forced to schema-registry type
    assert "vnd.schemaregistry" not in response.headers["content-type"]


# ---- Karapace counter ----


def test_karapace_requests_counter_increments() -> None:
    config = Config()
    client = _client(config)

    client.get("/subjects")
    client.get("/subjects")

    response = client.get("/metrics")

    assert response.status_code == 200
    assert 'karapace_http_requests_total{method="GET",path="/subjects",status="200"}' in response.text
