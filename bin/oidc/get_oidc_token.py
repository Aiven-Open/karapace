#!/usr/bin/env python3
"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

import sys
import time
from typing import Any, Literal, cast

import httpx
from pydantic import Field, SecretStr, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


DEFAULT_KEYCLOAK_URL = "http://keycloak:8080"
DEFAULT_PINGFEDERATE_TOKEN_URL = "https://pingfederate:9031/as/token.oauth2"
DEFAULT_PINGFEDERATE_CLIENT_SECRET = "karapace-secret"
DEFAULT_REALM = "karapace"
DEFAULT_CLIENT_ID = "karapace-client"
DEFAULT_SCOPE = "openid"
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_READY_TIMEOUT_SECONDS = 60.0
DEFAULT_RETRY_INTERVAL_SECONDS = 2.0

Provider = Literal["keycloak", "pingfederate"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=True, extra="ignore", env_ignore_empty=True)

    provider: Provider = Field(default="keycloak", alias="OIDC_PROVIDER")
    verify_tls: bool = Field(default=True, alias="OIDC_VERIFY_TLS")
    keycloak_url: str = Field(default=DEFAULT_KEYCLOAK_URL, alias="KEYCLOAK_URL")
    realm: str = Field(default=DEFAULT_REALM, alias="OIDC_REALM")
    keycloak_admin_user: str = Field(default="admin", alias="KEYCLOAK_ADMIN")
    keycloak_admin_password: SecretStr = Field(default=SecretStr("admin"), alias="KEYCLOAK_ADMIN_PASSWORD")
    oidc_client_id: str = Field(default=DEFAULT_CLIENT_ID, alias="OIDC_CLIENT_ID")
    oidc_client_secret: SecretStr | None = Field(default=None, alias="OIDC_CLIENT_SECRET")
    oidc_scope: str = Field(default=DEFAULT_SCOPE, alias="OIDC_SCOPE")
    oidc_token_url_override: str | None = Field(default=None, alias="OIDC_TOKEN_URL")

    @field_validator("provider", mode="before")
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
        return f"{self.keycloak_url}/realms/master/protocol/openid-connect/token"

    @property
    def keycloak_client_lookup_url(self) -> str:
        return f"{self.keycloak_url}/admin/realms/{self.realm}/clients"

    def keycloak_client_secret_url(self, client_uuid: str) -> str:
        return f"{self.keycloak_url}/admin/realms/{self.realm}/clients/{client_uuid}/client-secret"

    @computed_field(return_type=str)
    @property
    def oidc_token_url(self) -> str:
        if self.oidc_token_url_override:
            return self.oidc_token_url_override
        if self.provider == "pingfederate":
            return DEFAULT_PINGFEDERATE_TOKEN_URL
        return f"{self.keycloak_url}/realms/{self.realm}/protocol/openid-connect/token"


class TokenClient:
    def __init__(self, settings: Settings, http_client: httpx.Client) -> None:
        self.settings = settings
        self.http_client = http_client

    def _request_with_retry(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        end_time = time.monotonic() + DEFAULT_READY_TIMEOUT_SECONDS

        while True:
            try:
                response = self.http_client.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except httpx.HTTPError:
                if time.monotonic() >= end_time:
                    raise
                time.sleep(DEFAULT_RETRY_INTERVAL_SECONDS)

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
            "username": self.settings.keycloak_admin_user,
            "password": self.settings.keycloak_admin_password.get_secret_value(),
        }
        return str(self.post_form(self.settings.keycloak_admin_token_url, data)["access_token"])

    def get_keycloak_client_uuid(self, admin_token: str) -> str:
        clients = self.get_json(
            self.settings.keycloak_client_lookup_url,
            headers={"Authorization": f"Bearer {admin_token}"},
            params={"clientId": self.settings.oidc_client_id},
        )
        if not isinstance(clients, list) or not clients:
            raise SystemExit(f"Keycloak client not found: {self.settings.oidc_client_id}")
        client_uuid = clients[0].get("id")
        if not isinstance(client_uuid, str) or not client_uuid:
            raise SystemExit(f"Keycloak client lookup returned no id for: {self.settings.oidc_client_id}")
        return client_uuid

    def get_keycloak_client_secret(self, client_uuid: str, admin_token: str) -> str:
        payload = self.get_json(
            self.settings.keycloak_client_secret_url(client_uuid),
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        client_secret = payload.get("value") if isinstance(payload, dict) else None
        if not isinstance(client_secret, str) or not client_secret:
            raise SystemExit(f"Keycloak client secret not found for: {self.settings.oidc_client_id}")
        return client_secret

    def get_oidc_token(self, client_secret: str) -> str:
        data = {
            "grant_type": "client_credentials",
            "client_id": self.settings.oidc_client_id,
            "client_secret": client_secret,
            "scope": self.settings.oidc_scope,
        }
        return str(self.post_form(self.settings.oidc_token_url, data)["access_token"])


def resolve_client_secret(settings: Settings, client: TokenClient) -> str:
    if settings.oidc_client_secret is not None:
        return settings.oidc_client_secret.get_secret_value()
    if settings.provider == "pingfederate":
        return DEFAULT_PINGFEDERATE_CLIENT_SECRET
    admin_token = client.get_admin_token()
    client_uuid = client.get_keycloak_client_uuid(admin_token)
    return client.get_keycloak_client_secret(client_uuid, admin_token)


def main() -> int:
    settings = Settings()
    with httpx.Client(timeout=DEFAULT_TIMEOUT_SECONDS, verify=settings.verify_tls) as http_client:
        client = TokenClient(settings, http_client)
        client_secret = resolve_client_secret(settings, client)
        token = client.get_oidc_token(client_secret)
    print(token)
    return 0


if __name__ == "__main__":
    sys.exit(main())
