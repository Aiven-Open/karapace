#!/usr/bin/env python3
"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

import sys
import time
from typing import Any, Literal, cast

import httpx
from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

Provider = Literal["keycloak", "pingfederate"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=True, extra="ignore", env_ignore_empty=True)

    OIDC_PROVIDER: Provider = "keycloak"
    OIDC_VERIFY_TLS: bool = True
    KEYCLOAK_URL: str = "http://keycloak:8080"
    OIDC_REALM: str = "karapace"
    KEYCLOAK_ADMIN: str = "admin"
    KEYCLOAK_ADMIN_PASSWORD: SecretStr = SecretStr("admin")
    OIDC_CLIENT_ID: str = "karapace-client"
    OIDC_CLIENT_SECRET: SecretStr | None = None
    OIDC_SCOPE: str = "openid"
    OIDC_TOKEN_URL: str | None = None
    OIDC_DEFAULT_PINGFEDERATE_TOKEN_URL: str = "https://pingfederate:9031/as/token.oauth2"
    OIDC_DEFAULT_PINGFEDERATE_CLIENT_SECRET: str = "karapace-secret"
    OIDC_REQUEST_TIMEOUT_SECONDS: float = 30.0
    OIDC_READY_TIMEOUT_SECONDS: float = 60.0
    OIDC_RETRY_INTERVAL_SECONDS: float = 2.0

    @field_validator("OIDC_PROVIDER", mode="before")
    @classmethod
    def normalize_provider(cls, value: object) -> Provider:
        if not isinstance(value, str):
            return "keycloak"
        normalized = value.strip().lower()
        if normalized in {"pingfederate", "pingidentity"}:
            return "pingfederate"
        return "keycloak"

    @property
    def keycloak_admin_token_url(self) -> str:
        return f"{self.KEYCLOAK_URL}/realms/master/protocol/openid-connect/token"

    @property
    def keycloak_client_lookup_url(self) -> str:
        return f"{self.KEYCLOAK_URL}/admin/realms/{self.OIDC_REALM}/clients"

    def keycloak_client_secret_url(self, client_uuid: str) -> str:
        return f"{self.KEYCLOAK_URL}/admin/realms/{self.OIDC_REALM}/clients/{client_uuid}/client-secret"

    @property
    def resolved_oidc_token_url(self) -> str:
        if self.OIDC_TOKEN_URL:
            return self.OIDC_TOKEN_URL
        if self.OIDC_PROVIDER == "pingfederate":
            return self.OIDC_DEFAULT_PINGFEDERATE_TOKEN_URL
        return f"{self.KEYCLOAK_URL}/realms/{self.OIDC_REALM}/protocol/openid-connect/token"


class TokenClient:
    def __init__(self, settings: Settings, http_client: httpx.Client) -> None:
        self.settings = settings
        self.http_client = http_client

    def _request_with_retry(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        end_time = time.monotonic() + self.settings.OIDC_READY_TIMEOUT_SECONDS

        while True:
            try:
                response = self.http_client.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except httpx.HTTPError:
                if time.monotonic() >= end_time:
                    raise
                time.sleep(self.settings.OIDC_RETRY_INTERVAL_SECONDS)

    def post_form(self, url: str, data: dict[str, str]) -> dict[str, Any]:
        response = self._request_with_retry("POST", url, data=data)
        return cast(dict[str, Any], response.json())

    def get_json(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
    ) -> Any:
        response = self._request_with_retry("GET", url, headers=headers, params=params)
        return response.json()

    def get_admin_token(self) -> str:
        data = {
            "grant_type": "password",
            "client_id": "admin-cli",
            "username": self.settings.KEYCLOAK_ADMIN,
            "password": self.settings.KEYCLOAK_ADMIN_PASSWORD.get_secret_value(),
        }
        return str(self.post_form(self.settings.keycloak_admin_token_url, data)["access_token"])

    def get_keycloak_client_uuid(self, admin_token: str) -> str:
        clients = self.get_json(
            self.settings.keycloak_client_lookup_url,
            headers={"Authorization": f"Bearer {admin_token}"},
            params={"clientId": self.settings.OIDC_CLIENT_ID},
        )
        if not isinstance(clients, list) or not clients:
            raise SystemExit(f"Keycloak client not found: {self.settings.OIDC_CLIENT_ID}")
        client_uuid = clients[0].get("id")
        if not isinstance(client_uuid, str) or not client_uuid:
            raise SystemExit(f"Keycloak client lookup returned no id for: {self.settings.OIDC_CLIENT_ID}")
        return client_uuid

    def get_keycloak_client_secret(self, client_uuid: str, admin_token: str) -> str:
        payload = self.get_json(
            self.settings.keycloak_client_secret_url(client_uuid),
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        client_secret = payload.get("value") if isinstance(payload, dict) else None
        if not isinstance(client_secret, str) or not client_secret:
            raise SystemExit(f"Keycloak client secret not found for: {self.settings.OIDC_CLIENT_ID}")
        return client_secret

    def get_oidc_token(self, client_secret: str) -> str:
        data = {
            "grant_type": "client_credentials",
            "client_id": self.settings.OIDC_CLIENT_ID,
            "client_secret": client_secret,
            "scope": self.settings.OIDC_SCOPE,
        }
        return str(self.post_form(self.settings.resolved_oidc_token_url, data)["access_token"])


def resolve_client_secret(settings: Settings, client: TokenClient) -> str:
    if settings.OIDC_CLIENT_SECRET is not None:
        return settings.OIDC_CLIENT_SECRET.get_secret_value()
    if settings.OIDC_PROVIDER == "pingfederate":
        return settings.OIDC_DEFAULT_PINGFEDERATE_CLIENT_SECRET
    admin_token = client.get_admin_token()
    client_uuid = client.get_keycloak_client_uuid(admin_token)
    return client.get_keycloak_client_secret(client_uuid, admin_token)


def main() -> int:
    settings = Settings()
    with httpx.Client(timeout=settings.OIDC_REQUEST_TIMEOUT_SECONDS, verify=settings.OIDC_VERIFY_TLS) as http_client:
        client = TokenClient(settings, http_client)
        client_secret = resolve_client_secret(settings, client)
        token = client.get_oidc_token(client_secret)
    print(token)
    return 0


if __name__ == "__main__":
    sys.exit(main())
