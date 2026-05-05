"""
schema_registry - SchemaRegistryRoute content negotiation tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from karapace.api.content_type import SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE
from karapace.api.routers.raw_path_router import SchemaRegistryRoute


@pytest.fixture
def app() -> FastAPI:
    router = APIRouter(prefix="/config", route_class=SchemaRegistryRoute)

    class CompatibilityRequest(BaseModel):
        compatibility: str

    @router.get("")
    async def config_get():
        return {"compatibilityLevel": "NONE"}

    @router.put("")
    async def config_put(body: CompatibilityRequest):
        return {"compatibility": body.compatibility}

    app = FastAPI()
    app.include_router(router)

    @app.get("/metrics")
    async def metrics():
        return "ok"

    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app, raise_server_exceptions=False)


class TestSchemaRegistryRouteContentNegotiation:
    def test_get_default_content_type(self, client: TestClient) -> None:
        response = client.get("/config")
        assert response.status_code == 200
        assert SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE in response.headers["Content-Type"]

    def test_get_with_valid_accept(self, client: TestClient) -> None:
        response = client.get("/config", headers={"Accept": "application/json"})
        assert response.status_code == 200
        assert "application/json" in response.headers["Content-Type"]

    def test_get_with_wildcard_accept(self, client: TestClient) -> None:
        response = client.get("/config", headers={"Accept": "*/*"})
        assert response.status_code == 200
        assert SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE in response.headers["Content-Type"]

    def test_get_with_invalid_accept_returns_406(self, client: TestClient) -> None:
        response = client.get("/config", headers={"Accept": "text/html"})
        assert response.status_code == 406
        assert SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE in response.headers["Content-Type"]
        assert response.json()["message"] == "HTTP 406 Not Acceptable"

    def test_put_with_invalid_content_type_returns_415(self, client: TestClient) -> None:
        response = client.put(
            "/config",
            content='{"compatibility": "NONE"}',
            headers={"Content-Type": "text/html"},
        )
        assert response.status_code == 415
        assert SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE in response.headers["Content-Type"]
        assert response.json()["message"] == "HTTP 415 Unsupported Media Type"

    def test_put_with_valid_content_type(self, client: TestClient) -> None:
        response = client.put(
            "/config",
            content='{"compatibility": "FULL"}',
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 200
        assert SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE in response.headers["Content-Type"]
        assert response.json()["compatibility"] == "FULL"

    def test_put_with_octet_stream_rewritten_to_json(self, client: TestClient) -> None:
        response = client.put(
            "/config",
            content='{"compatibility": "FULL"}',
            headers={"Content-Type": "application/octet-stream"},
        )
        assert response.status_code == 200
        assert SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE in response.headers["Content-Type"]

    def test_put_with_schema_registry_content_type(self, client: TestClient) -> None:
        response = client.put(
            "/config",
            content='{"compatibility": "FULL"}',
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        assert response.status_code == 200
        assert SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE in response.headers["Content-Type"]


class TestSchemaRegistryRouteDoesNotAffectOtherEndpoints:
    def test_non_schema_endpoint_with_text_accept(self, client: TestClient) -> None:
        """Non-schema-registry endpoints must not get 406 for non-schema Accept headers."""
        response = client.get("/metrics", headers={"Accept": "text/plain"})
        assert response.status_code == 200

    def test_non_schema_endpoint_with_prometheus_accept(self, client: TestClient) -> None:
        prometheus_accept = (
            "application/openmetrics-text;version=1.0.0,application/openmetrics-text;version=0.0.1;q=0.75,"
            "text/plain;version=0.0.4;q=0.5,*/*;q=0.1"
        )
        response = client.get("/metrics", headers={"Accept": prometheus_accept})
        assert response.status_code == 200
