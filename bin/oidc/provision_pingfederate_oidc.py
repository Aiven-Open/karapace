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
from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=True, extra="ignore")

    PINGFEDERATE_ADMIN_URL: str = "https://pingfederate:9999"
    PINGFEDERATE_ADMIN_USER: str = "administrator"
    PINGFEDERATE_ADMIN_PASSWORD: SecretStr = SecretStr("2FederateM0re")
    PINGFEDERATE_VERIFY_TLS: bool = False
    PINGFEDERATE_CLIENT_ID: str = "karapace-client"
    PINGFEDERATE_CLIENT_SECRET: SecretStr = SecretStr("karapace-secret")
    PINGFEDERATE_CLIENT_NAME: str = "Karapace Client"
    PINGFEDERATE_ATM_ID: str = "karapacejwtatm"
    PINGFEDERATE_ATM_NAME: str = "Karapace JWT ATM"
    PINGFEDERATE_TOKEN_ISSUER: str = "https://pingfederate:9031"
    PINGFEDERATE_TOKEN_AUDIENCE: str = "karapace-audience"
    PINGFEDERATE_CLIENT_ID_CLAIM: str = "client_id"
    PINGFEDERATE_ROLES_CLAIM: str = "roles"
    PINGFEDERATE_ROLE_VALUES: list[str] = (
        "schema:read,schema:write,schema:delete,subject:read,subject:write,subject:delete,"
        "config_subject:update,config_global:update"
    )
    PINGFEDERATE_READY_TIMEOUT_SECONDS: int = 180
    PINGFEDERATE_READY_INTERVAL_SECONDS: float = 5.0

    @field_validator("PINGFEDERATE_ROLE_VALUES", mode="before")
    @classmethod
    def parse_role_values(cls, value: object) -> list[str]:
        if value is None:
            value = cls.model_fields["PINGFEDERATE_ROLE_VALUES"].default
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        if isinstance(value, list):
            return [item.strip() for item in value if isinstance(item, str) and item.strip()]
        raise ValueError("PINGFEDERATE_ROLE_VALUES must be a comma-separated string or a list of strings")


class PingFederateAdmin:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = httpx.Client(
            auth=(settings.PINGFEDERATE_ADMIN_USER, settings.PINGFEDERATE_ADMIN_PASSWORD.get_secret_value()),
            base_url=settings.PINGFEDERATE_ADMIN_URL,
            headers={
                "X-XSRF-Header": "PingFederate",
                "Content-Type": "application/json",
            },
            timeout=60.0,
            verify=settings.PINGFEDERATE_VERIFY_TLS,
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
    deadline = time.monotonic() + settings.PINGFEDERATE_READY_TIMEOUT_SECONDS
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
        time.sleep(settings.PINGFEDERATE_READY_INTERVAL_SECONDS)

    raise SystemExit(
        "Timed out waiting for PingFederate admin API readiness after "
        f"{settings.PINGFEDERATE_READY_TIMEOUT_SECONDS}s: {last_error}"
    )


def upsert_access_token_manager(admin: PingFederateAdmin) -> None:
    settings = admin.settings
    descriptor_id = "com.pingidentity.pf.access.token.management.plugins.JwtBearerAccessTokenManagementPlugin"
    fields = [
        {"name": "Token Lifetime", "value": "120"},
        {"name": "Use Centralized Signing Key", "value": "true"},
        {"name": "JWS Algorithm", "value": "RS256"},
        {"name": "Issuer Claim Value", "value": settings.PINGFEDERATE_TOKEN_ISSUER},
        {"name": "Audience Claim Value", "value": settings.PINGFEDERATE_TOKEN_AUDIENCE},
        {"name": "Type Header Value", "value": "at+jwt"},
        {"name": "Client ID Claim Name", "value": settings.PINGFEDERATE_CLIENT_ID_CLAIM},
    ]
    payload = {
        "id": settings.PINGFEDERATE_ATM_ID,
        "name": settings.PINGFEDERATE_ATM_NAME,
        "pluginDescriptorRef": resource_link(descriptor_id),
        "configuration": {"fields": fields, "tables": []},
        "attributeContract": {
            "extendedAttributes": [
                {"name": settings.PINGFEDERATE_ROLES_CLAIM, "multiValued": True},
            ]
        },
        "tokenEndpointAttributeContract": {"attributes": []},
        "selectionSettings": {"resourceUris": []},
        "accessControlSettings": {"restrictClients": False, "allowedClients": []},
        "sessionValidationSettings": {"includeSessionId": False},
    }

    managers_payload = admin.get_json("/pf-admin-api/v1/oauth/accessTokenManagers")
    managers = managers_payload.get("items", []) if isinstance(managers_payload, dict) else []
    if any(item.get("id") == settings.PINGFEDERATE_ATM_ID for item in managers):
        admin.put_json(f"/pf-admin-api/v1/oauth/accessTokenManagers/{settings.PINGFEDERATE_ATM_ID}", payload)
        return
    admin.post_json("/pf-admin-api/v1/oauth/accessTokenManagers", payload)


def upsert_client(admin: PingFederateAdmin) -> None:
    settings = admin.settings
    payload = {
        "clientId": settings.PINGFEDERATE_CLIENT_ID,
        "name": settings.PINGFEDERATE_CLIENT_NAME,
        "grantTypes": ["CLIENT_CREDENTIALS"],
        "enabled": True,
        "clientAuth": {"type": "SECRET", "secret": settings.PINGFEDERATE_CLIENT_SECRET.get_secret_value()},
        "defaultAccessTokenManagerRef": resource_link(settings.PINGFEDERATE_ATM_ID),
        "restrictToDefaultAccessTokenManager": True,
        "restrictScopes": False,
    }

    clients_payload = admin.get_json("/pf-admin-api/v1/oauth/clients")
    clients = clients_payload.get("items", []) if isinstance(clients_payload, dict) else []
    if any(item.get("clientId") == settings.PINGFEDERATE_CLIENT_ID for item in clients):
        admin.put_json(f"/pf-admin-api/v1/oauth/clients/{settings.PINGFEDERATE_CLIENT_ID}", payload)
        return
    admin.post_json("/pf-admin-api/v1/oauth/clients", payload)


def upsert_client_credentials_mapping(admin: PingFederateAdmin) -> None:
    settings = admin.settings
    serialized_roles = " ".join(settings.PINGFEDERATE_ROLE_VALUES)
    payload = {
        "context": {"type": "CLIENT_CREDENTIALS"},
        "accessTokenManagerRef": resource_link(settings.PINGFEDERATE_ATM_ID),
        "attributeSources": [],
        "attributeContractFulfillment": {
            settings.PINGFEDERATE_ROLES_CLAIM: {"source": {"type": "TEXT"}, "value": serialized_roles},
        },
    }

    mappings_payload = admin.get_json("/pf-admin-api/v1/oauth/accessTokenMappings")
    mappings = mappings_payload if isinstance(mappings_payload, list) else mappings_payload.get("items", [])
    existing = next(
        (
            item
            for item in mappings
            if item.get("context", {}).get("type") == "CLIENT_CREDENTIALS"
            and item.get("accessTokenManagerRef", {}).get("id") == settings.PINGFEDERATE_ATM_ID
        ),
        None,
    )
    if existing:
        mapping_id = existing.get("id")
        if isinstance(mapping_id, str) and mapping_id:
            existing_claims = existing.get("attributeContractFulfillment", {})
            if settings.PINGFEDERATE_ROLES_CLAIM not in existing_claims and existing_claims:
                existing_claim_names = ", ".join(sorted(existing_claims.keys()))
                raise SystemExit(
                    "Existing PingFederate access token mapping uses a different roles claim "
                    f"({existing_claim_names}). Recreate the PingFederate stack from a clean state "
                    "before switching roles claim names."
                )
            existing_roles = existing.get("attributeContractFulfillment", {}).get(settings.PINGFEDERATE_ROLES_CLAIM, {})
            expected_roles = payload["attributeContractFulfillment"][settings.PINGFEDERATE_ROLES_CLAIM]
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
                    "client_id": settings.PINGFEDERATE_CLIENT_ID,
                    "client_secret": settings.PINGFEDERATE_CLIENT_SECRET.get_secret_value(),
                    "token_issuer": settings.PINGFEDERATE_TOKEN_ISSUER,
                    "token_audience": settings.PINGFEDERATE_TOKEN_AUDIENCE,
                    "karapace_sub_claim_name": settings.PINGFEDERATE_CLIENT_ID_CLAIM,
                    "roles_claim": settings.PINGFEDERATE_ROLES_CLAIM,
                    "role_values": settings.PINGFEDERATE_ROLE_VALUES,
                    "serialized_role_value": " ".join(settings.PINGFEDERATE_ROLE_VALUES),
                },
                indent=2,
            )
        )
    finally:
        admin.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
