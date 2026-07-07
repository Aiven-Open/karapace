#!/usr/bin/env python3
"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details

Fetches an OIDC access token for use against the Schema Registry. Can be run
ad-hoc by developers to grab a token for CLI/API calls.

Provider is selected by OIDC_PROVIDER (default: keycloak). The keycloak default
looks up the client secret via the admin API. The cloud providers (pingfederate,
entra) fetch the token directly with client_credentials using the supplied
OIDC_TOKEN_URL and OIDC_CLIENT_SECRET.
"""

import os
import requests

KNOWN_PROVIDERS = ("keycloak", "pingfederate", "entra")
PROVIDER = os.environ.get("OIDC_PROVIDER", "keycloak").strip().lower()
KEYCLOAK_URL = "http://keycloak:8080"
REALM = os.environ.get("OIDC_REALM", "karapace")
CLIENT_ID = os.environ.get("OIDC_CLIENT_ID", "karapace-client")
CLIENT_SECRET = os.environ.get("OIDC_CLIENT_SECRET")
SCOPE = os.environ.get("OIDC_SCOPE", "openid")
TOKEN_URL = os.environ.get("OIDC_TOKEN_URL")
VERIFY_TLS = os.environ.get("OIDC_VERIFY_TLS", "true").strip().lower() not in ("0", "false", "no", "off")
ADMIN_USER = os.environ.get("KEYCLOAK_ADMIN", "admin")
ADMIN_PASS = os.environ.get("KEYCLOAK_ADMIN_PASSWORD", "admin")


def get_admin_token():
    url = f"{KEYCLOAK_URL}/realms/master/protocol/openid-connect/token"
    data = {
        "grant_type": "password",
        "client_id": "admin-cli",
        "username": ADMIN_USER,
        "password": ADMIN_PASS,
    }
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_client_uuid(admin_token):
    url = f"{KEYCLOAK_URL}/admin/realms/{REALM}/clients?clientId={CLIENT_ID}"
    headers = {"Authorization": f"Bearer {admin_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()[0]["id"]


def get_client_secret(client_uuid, admin_token):
    url = f"{KEYCLOAK_URL}/admin/realms/{REALM}/clients/{client_uuid}/client-secret"
    headers = {"Authorization": f"Bearer {admin_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()["value"]


def get_oidc_token(token_url, client_secret):
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": client_secret,
        "scope": SCOPE,
    }
    resp = requests.post(token_url, data=data, verify=VERIFY_TLS)
    resp.raise_for_status()
    return resp.json()["access_token"]


def resolve_keycloak():
    admin_token = get_admin_token()
    client_uuid = get_client_uuid(admin_token)
    client_secret = CLIENT_SECRET or get_client_secret(client_uuid, admin_token)
    token_url = TOKEN_URL or f"{KEYCLOAK_URL}/realms/{REALM}/protocol/openid-connect/token"
    return token_url, client_secret


def resolve_cloud():
    if not TOKEN_URL or not CLIENT_SECRET:
        raise SystemExit(f"OIDC_TOKEN_URL and OIDC_CLIENT_SECRET are required when OIDC_PROVIDER={PROVIDER}")
    return TOKEN_URL, CLIENT_SECRET


if __name__ == "__main__":
    if PROVIDER not in KNOWN_PROVIDERS:
        raise SystemExit(f"Unknown OIDC_PROVIDER={PROVIDER!r}; expected one of {', '.join(KNOWN_PROVIDERS)}")
    if PROVIDER == "keycloak":
        token_url, client_secret = resolve_keycloak()
    else:
        token_url, client_secret = resolve_cloud()
    print(get_oidc_token(token_url, client_secret))
