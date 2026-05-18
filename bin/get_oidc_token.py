#!/usr/bin/env python3
"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

import os
import sys
import time
import requests
from requests.exceptions import RequestException
from urllib.parse import urlsplit, urlunsplit

KEYCLOAK_URL = os.environ.get("KEYCLOAK_URL", "http://keycloak:8080")
KEYCLOAK_CONNECT_URL = os.environ.get("KEYCLOAK_CONNECT_URL", KEYCLOAK_URL)
KEYCLOAK_REQUEST_TIMEOUT_SECONDS = float(os.environ.get("KEYCLOAK_REQUEST_TIMEOUT_SECONDS", "5"))
KEYCLOAK_STARTUP_TIMEOUT_SECONDS = float(os.environ.get("KEYCLOAK_STARTUP_TIMEOUT_SECONDS", "60"))
KEYCLOAK_RETRY_DELAY_SECONDS = float(os.environ.get("KEYCLOAK_RETRY_DELAY_SECONDS", "1"))
KEYCLOAK_RETRYABLE_STATUS_CODES = {502, 503}
REALM = "karapace"
CLIENT_ID = "karapace-client"
ADMIN_USER = os.environ.get("KEYCLOAK_ADMIN", "admin")
ADMIN_PASS = os.environ.get("KEYCLOAK_ADMIN_PASSWORD", "admin")


def _build_request_url(path):
    connect = urlsplit(KEYCLOAK_CONNECT_URL)
    connect_path = connect.path.rstrip("/")
    request_path = path if path.startswith("/") else f"/{path}"
    full_path = f"{connect_path}{request_path}" if connect_path else request_path
    return urlunsplit((connect.scheme, connect.netloc, full_path, "", ""))


def _request(method, path, **kwargs):
    logical = urlsplit(KEYCLOAK_URL)
    connect = urlsplit(KEYCLOAK_CONNECT_URL)
    url = _build_request_url(path)

    headers = dict(kwargs.pop("headers", {}))
    if connect.netloc != logical.netloc:
        headers["Host"] = logical.netloc

    deadline = time.monotonic() + KEYCLOAK_STARTUP_TIMEOUT_SECONDS
    last_exception = None

    while time.monotonic() < deadline:
        try:
            response = requests.request(
                method,
                url,
                headers=headers,
                timeout=KEYCLOAK_REQUEST_TIMEOUT_SECONDS,
                **kwargs,
            )
            if response.status_code not in KEYCLOAK_RETRYABLE_STATUS_CODES:
                return response
            last_exception = RuntimeError(f"Keycloak returned retryable status {response.status_code} for {url}")
        except RequestException as exc:
            last_exception = exc

        time.sleep(KEYCLOAK_RETRY_DELAY_SECONDS)

    raise TimeoutError(
        f"Keycloak did not become ready for {url} within {KEYCLOAK_STARTUP_TIMEOUT_SECONDS} seconds"
    ) from last_exception


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
    try:
        admin_token = get_admin_token()
        client_uuid = get_client_uuid(admin_token)
        client_secret = get_client_secret(client_uuid, admin_token)
        token = get_oidc_token(client_secret)
        print(token)
    except Exception as exc:
        print(f"get_oidc_token.py: {exc}", file=sys.stderr)
        raise SystemExit(1)
