#!/usr/bin/env python3
"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

import os
import requests
from urllib.parse import urlsplit, urlunsplit

KEYCLOAK_URL = os.environ.get("KEYCLOAK_URL", "http://keycloak:8080")
KEYCLOAK_CONNECT_URL = os.environ.get("KEYCLOAK_CONNECT_URL", KEYCLOAK_URL)
REALM = "karapace"
CLIENT_ID = "karapace-client"
ADMIN_USER = os.environ.get("KEYCLOAK_ADMIN", "admin")
ADMIN_PASS = os.environ.get("KEYCLOAK_ADMIN_PASSWORD", "admin")


def _request(method, path, **kwargs):
    logical = urlsplit(KEYCLOAK_URL)
    connect = urlsplit(KEYCLOAK_CONNECT_URL)
    url = urlunsplit((connect.scheme, connect.netloc, path, "", ""))

    headers = dict(kwargs.pop("headers", {}))
    if connect.netloc != logical.netloc:
        headers["Host"] = logical.netloc

    return requests.request(method, url, headers=headers, **kwargs)


def get_admin_token():
    data = {
        "grant_type": "password",
        "client_id": "admin-cli",
        "username": ADMIN_USER,
        "password": ADMIN_PASS,
    }
    resp = _request("POST", "/realms/master/protocol/openid-connect/token", data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_client_uuid(admin_token):
    headers = {"Authorization": f"Bearer {admin_token}"}
    resp = _request("GET", f"/admin/realms/{REALM}/clients", headers=headers, params={"clientId": CLIENT_ID})
    resp.raise_for_status()
    return resp.json()[0]["id"]


def get_client_secret(client_uuid, admin_token):
    headers = {"Authorization": f"Bearer {admin_token}"}
    resp = _request("GET", f"/admin/realms/{REALM}/clients/{client_uuid}/client-secret", headers=headers)
    resp.raise_for_status()
    return resp.json()["value"]


def get_oidc_token(client_secret):
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": client_secret,
        "scope": "openid",
    }
    resp = _request("POST", f"/realms/{REALM}/protocol/openid-connect/token", data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]


if __name__ == "__main__":
    admin_token = get_admin_token()
    client_uuid = get_client_uuid(admin_token)
    client_secret = get_client_secret(client_uuid, admin_token)
    token = get_oidc_token(client_secret)
    print(token)
