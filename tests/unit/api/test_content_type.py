"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from fastapi import HTTPException, Request, status
from karapace.api.content_type import (
    JSON_CONTENT_TYPE,
    SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE,
    negotiate_schema_content_type,
)

import pytest


def _request(method: str, headers: dict[str, str] | None = None) -> Request:
    headers = headers or {}
    raw_headers = [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers.items()]
    scope = {
        "type": "http",
        "method": method,
        "headers": raw_headers,
        "path": "/",
        "query_string": b"",
    }
    return Request(scope)


@pytest.mark.parametrize(
    "content_type",
    [
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json",
        "application/json",
        "application/octet-stream",
    ],
)
def test_post_accepts_allowed_content_types(content_type: str) -> None:
    req = _request("POST", {"Content-Type": content_type})
    assert negotiate_schema_content_type(req) == SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE


@pytest.mark.parametrize("method", ["POST", "PUT"])
def test_post_put_reject_unsupported_content_type(method: str) -> None:
    req = _request(method, {"Content-Type": "text/plain"})

    with pytest.raises(HTTPException) as exc_info:
        negotiate_schema_content_type(req)

    assert exc_info.value.status_code == status.HTTP_415_UNSUPPORTED_MEDIA_TYPE


def test_get_ignores_content_type_check() -> None:
    req = _request("GET", {"Content-Type": "text/plain"})
    assert negotiate_schema_content_type(req) == SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE


def test_missing_content_type_defaults_to_json() -> None:
    req = _request("POST")
    assert negotiate_schema_content_type(req) == SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE


def test_content_type_with_parameters_is_accepted() -> None:
    req = _request("POST", {"Content-Type": f"{JSON_CONTENT_TYPE}; charset=utf-8"})
    assert negotiate_schema_content_type(req) == SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE


@pytest.mark.parametrize(
    "accept,expected",
    [
        ("application/vnd.schemaregistry.v1+json", "application/vnd.schemaregistry.v1+json"),
        ("application/vnd.schemaregistry+json", "application/vnd.schemaregistry+json"),
        ("application/json", "application/json"),
    ],
)
def test_accept_header_returns_matching_type(accept: str, expected: str) -> None:
    req = _request("GET", {"Accept": accept})
    assert negotiate_schema_content_type(req) == expected


@pytest.mark.parametrize("accept", ["*/*", "*", "*/json"])
def test_wildcard_accept_returns_default(accept: str) -> None:
    req = _request("GET", {"Accept": accept})
    assert negotiate_schema_content_type(req) == SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE


def test_unsupported_accept_raises_406() -> None:
    req = _request("GET", {"Accept": "application/xml"})

    with pytest.raises(HTTPException) as exc_info:
        negotiate_schema_content_type(req)

    assert exc_info.value.status_code == status.HTTP_406_NOT_ACCEPTABLE


def test_missing_accept_returns_default() -> None:
    req = _request("GET", {"Content-Type": JSON_CONTENT_TYPE})
    assert negotiate_schema_content_type(req) == SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE


def test_accept_prefers_most_specific_match() -> None:
    req = _request(
        "GET",
        {"Accept": "application/json;q=0.5, application/vnd.schemaregistry.v1+json;q=0.9"},
    )
    assert negotiate_schema_content_type(req) == "application/vnd.schemaregistry.v1+json"
