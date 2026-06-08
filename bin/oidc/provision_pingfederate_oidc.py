#!/usr/bin/env python3
"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

import json
import sys
import time
from typing import Any

import httpx
from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


DEFAULT_BASE_URL = "https://pingfederate:9999"
DEFAULT_ADMIN_USER = "administrator"
DEFAULT_ADMIN_PASSWORD = "2FederateM0re"
DEFAULT_CLIENT_ID = "karapace-client"
DEFAULT_CLIENT_SECRET = "karapace-secret"
DEFAULT_CLIENT_NAME = "Karapace Client"
DEFAULT_ATM_ID = "karapacejwtatm"
DEFAULT_ATM_NAME = "Karapace JWT ATM"
DEFAULT_ISSUER = "https://pingfederate:9031"
DEFAULT_AUDIENCE = "karapace-audience"
DEFAULT_CLIENT_ID_CLAIM = "client_id"
DEFAULT_ROLES_CLAIM = "karapace.roles"
DEFAULT_ROLE_VALUES = (
    "schema:read,schema:write,schema:delete,subject:read,subject:write,subject:delete,"
    "config_subject:update,config_global:update"
)
DEFAULT_READY_TIMEOUT_SECONDS = 180
DEFAULT_READY_INTERVAL_SECONDS = 5.0


class Settings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=True, extra="ignore")

    base_url: str = Field(default=DEFAULT_BASE_URL, alias="PINGFEDERATE_ADMIN_URL")
    admin_user: str = Field(default=DEFAULT_ADMIN_USER, alias="PINGFEDERATE_ADMIN_USER")
    admin_password: SecretStr = Field(default=SecretStr(DEFAULT_ADMIN_PASSWORD), alias="PINGFEDERATE_ADMIN_PASSWORD")
    verify_tls: bool = Field(default=False, alias="PINGFEDERATE_VERIFY_TLS")
    client_id: str = Field(default=DEFAULT_CLIENT_ID, alias="PINGFEDERATE_CLIENT_ID")
    client_secret: SecretStr = Field(default=SecretStr(DEFAULT_CLIENT_SECRET), alias="PINGFEDERATE_CLIENT_SECRET")
    client_name: str = Field(default=DEFAULT_CLIENT_NAME, alias="PINGFEDERATE_CLIENT_NAME")
    atm_id: str = Field(default=DEFAULT_ATM_ID, alias="PINGFEDERATE_ATM_ID")
    atm_name: str = Field(default=DEFAULT_ATM_NAME, alias="PINGFEDERATE_ATM_NAME")
    issuer: str = Field(default=DEFAULT_ISSUER, alias="PINGFEDERATE_TOKEN_ISSUER")
    audience: str = Field(default=DEFAULT_AUDIENCE, alias="PINGFEDERATE_TOKEN_AUDIENCE")
    client_id_claim: str = Field(default=DEFAULT_CLIENT_ID_CLAIM, alias="PINGFEDERATE_CLIENT_ID_CLAIM")
    roles_claim: str = Field(default=DEFAULT_ROLES_CLAIM, alias="PINGFEDERATE_ROLES_CLAIM")
    role_values: list[str] = Field(default=DEFAULT_ROLE_VALUES, alias="PINGFEDERATE_ROLE_VALUES")
    ready_timeout_seconds: int = Field(
        default=DEFAULT_READY_TIMEOUT_SECONDS,
        alias="PINGFEDERATE_READY_TIMEOUT_SECONDS",
    )
    ready_interval_seconds: float = Field(
        default=DEFAULT_READY_INTERVAL_SECONDS,
        alias="PINGFEDERATE_READY_INTERVAL_SECONDS",
    )

    @field_validator("role_values", mode="before")
    @classmethod
    def parse_role_values(cls, value: object) -> list[str]:
        if value is None:
            value = DEFAULT_ROLE_VALUES
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        if isinstance(value, list):
            return [item.strip() for item in value if isinstance(item, str) and item.strip()]
        raise ValueError("PINGFEDERATE_ROLE_VALUES must be a comma-separated string or a list of strings")


class PingFederateAdmin:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = httpx.Client(
            auth=(settings.admin_user, settings.admin_password.get_secret_value()),
            base_url=settings.base_url,
            headers={
                "X-XSRF-Header": "PingFederate",
                "Content-Type": "application/json",
            },
            timeout=60.0,
            verify=settings.verify_tls,
        )

    def request(self, method: str, path: str, expected: tuple[int, ...], body: dict[str, Any] | None = None) -> Any:
        response = self.client.request(method, path, json=body)
        if response.status_code not in expected:
            raise SystemExit(f"{method} {path} failed with {response.status_code}: {response.text}")
        if not response.content:
            return None
        return response.json()

    def close(self) -> None:
        self.client.close()

    def get_json(self, path: str) -> Any:
        return self.request("GET", path, (200,))

    def post_json(self, path: str, body: dict[str, Any]) -> Any:
        return self.request("POST", path, (200, 201), body)

    def put_json(self, path: str, body: dict[str, Any]) -> Any:
        return self.request("PUT", path, (200,), body)


def resource_link(resource_id: str) -> dict[str, str]:
    return {"id": resource_id}


def wait_until_ready(admin: PingFederateAdmin) -> None:
    settings = admin.settings
    deadline = time.monotonic() + settings.ready_timeout_seconds
    last_error = "unknown error"

    while time.monotonic() < deadline:
        try:
            response = admin.client.get("/pf-admin-api/v1/oauth/clients")
            if response.status_code == 200:
                return
            if response.status_code in {401, 403}:
                raise SystemExit(f"PingFederate admin API rejected credentials with {response.status_code}: {response.text}")
            last_error = f"HTTP {response.status_code}: {response.text}"
        except httpx.HTTPError as exc:
            last_error = str(exc)
        time.sleep(settings.ready_interval_seconds)

    raise SystemExit(
        "Timed out waiting for PingFederate admin API readiness after " f"{settings.ready_timeout_seconds}s: {last_error}"
    )


def upsert_access_token_manager(admin: PingFederateAdmin) -> None:
    settings = admin.settings
    descriptor_id = "com.pingidentity.pf.access.token.management.plugins.JwtBearerAccessTokenManagementPlugin"
    fields = [
        {"name": "Token Lifetime", "value": "120"},
        {"name": "Use Centralized Signing Key", "value": "true"},
        {"name": "JWS Algorithm", "value": "RS256"},
        {"name": "Issuer Claim Value", "value": settings.issuer},
        {"name": "Audience Claim Value", "value": settings.audience},
        {"name": "Type Header Value", "value": "at+jwt"},
        {"name": "Client ID Claim Name", "value": settings.client_id_claim},
    ]
    payload = {
        "id": settings.atm_id,
        "name": settings.atm_name,
        "pluginDescriptorRef": resource_link(descriptor_id),
        "configuration": {"fields": fields, "tables": []},
        "attributeContract": {
            "extendedAttributes": [
                {"name": settings.roles_claim, "multiValued": True},
            ]
        },
        "tokenEndpointAttributeContract": {"attributes": []},
        "selectionSettings": {"resourceUris": []},
        "accessControlSettings": {"restrictClients": False, "allowedClients": []},
        "sessionValidationSettings": {"includeSessionId": False},
    }

    managers_payload = admin.get_json("/pf-admin-api/v1/oauth/accessTokenManagers")
    managers = managers_payload.get("items", []) if isinstance(managers_payload, dict) else []
    if any(item.get("id") == settings.atm_id for item in managers):
        admin.put_json(f"/pf-admin-api/v1/oauth/accessTokenManagers/{settings.atm_id}", payload)
        return
    admin.post_json("/pf-admin-api/v1/oauth/accessTokenManagers", payload)


def upsert_client(admin: PingFederateAdmin) -> None:
    settings = admin.settings
    payload = {
        "clientId": settings.client_id,
        "name": settings.client_name,
        "grantTypes": ["CLIENT_CREDENTIALS"],
        "enabled": True,
        "clientAuth": {"type": "SECRET", "secret": settings.client_secret.get_secret_value()},
        "defaultAccessTokenManagerRef": resource_link(settings.atm_id),
        "restrictToDefaultAccessTokenManager": True,
        "restrictScopes": False,
    }

    clients_payload = admin.get_json("/pf-admin-api/v1/oauth/clients")
    clients = clients_payload.get("items", []) if isinstance(clients_payload, dict) else []
    if any(item.get("clientId") == settings.client_id for item in clients):
        admin.put_json(f"/pf-admin-api/v1/oauth/clients/{settings.client_id}", payload)
        return
    admin.post_json("/pf-admin-api/v1/oauth/clients", payload)


def upsert_client_credentials_mapping(admin: PingFederateAdmin) -> None:
    settings = admin.settings
    payload = {
        "context": {"type": "CLIENT_CREDENTIALS"},
        "accessTokenManagerRef": resource_link(settings.atm_id),
        "attributeSources": [],
        "attributeContractFulfillment": {
            settings.roles_claim: {"source": {"type": "TEXT"}, "value": " ".join(settings.role_values)},
        },
    }

    mappings_payload = admin.get_json("/pf-admin-api/v1/oauth/accessTokenMappings")
    mappings = mappings_payload if isinstance(mappings_payload, list) else mappings_payload.get("items", [])
    existing = next(
        (
            item
            for item in mappings
            if item.get("context", {}).get("type") == "CLIENT_CREDENTIALS"
            and item.get("accessTokenManagerRef", {}).get("id") == settings.atm_id
        ),
        None,
    )
    if existing:
        mapping_id = existing.get("id")
        if isinstance(mapping_id, str) and mapping_id:
            existing_roles = existing.get("attributeContractFulfillment", {}).get(settings.roles_claim, {})
            expected_roles = payload["attributeContractFulfillment"][settings.roles_claim]
            if existing_roles == expected_roles:
                return
            admin.put_json(
                f"/pf-admin-api/v1/oauth/accessTokenMappings/{mapping_id}",
                {
                    "id": mapping_id,
                    "attributeSources": existing.get("attributeSources", []),
                    "issuanceCriteria": existing.get("issuanceCriteria", {"conditionalCriteria": []}),
                    **payload,
                },
            )
        return
    admin.post_json("/pf-admin-api/v1/oauth/accessTokenMappings", payload)


def main() -> int:
    settings = Settings()
    admin = PingFederateAdmin(settings)
    try:
        wait_until_ready(admin)
        upsert_access_token_manager(admin)
        upsert_client(admin)
        upsert_client_credentials_mapping(admin)
        print(
            json.dumps(
                {
                    "client_id": settings.client_id,
                    "client_secret": settings.client_secret.get_secret_value(),
                    "token_issuer": settings.issuer,
                    "token_audience": settings.audience,
                    "karapace_sub_claim_name": settings.client_id_claim,
                    "roles_claim": settings.roles_claim,
                    "role_values": settings.role_values,
                },
                indent=2,
            )
        )
    finally:
        admin.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
