"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

import os
import requests

KEYCLOAK_URL = "http://keycloak:8080"
REALM = "karapace"
CLIENT_ID = "karapace-client"
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


def get_oidc_token(client_secret):
    url = f"{KEYCLOAK_URL}/realms/{REALM}/protocol/openid-connect/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": client_secret,
        "scope": "openid",
    }
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]


if __name__ == "__main__":
    admin_token = get_admin_token()
    client_uuid = get_client_uuid(admin_token)
    client_secret = get_client_secret(client_uuid, admin_token)
    token = get_oidc_token(client_secret)
    print(token)
