"""
karapace - master forwarding tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import pytest
from fastapi import HTTPException, Request
from fastapi.datastructures import Headers
from pydantic import BaseModel
from starlette.datastructures import MutableHeaders

from karapace.api.forward_client import ForwardClient
from karapace.core.container import KarapaceContainer
from tests.base_testcase import BaseTestCase


class TestResponse(BaseModel):
    number: int
    string: str


@dataclass
class ContentTypeTestCase(BaseTestCase):
    content_type: str


@pytest.fixture(name="forward_client")
def fixture_forward_client(karapace_container: KarapaceContainer) -> ForwardClient:
    with patch("karapace.api.forward_client.aiohttp") as mocked_aiohttp:
        mocked_aiohttp.ClientSession.return_value = Mock(
            spec=aiohttp.ClientSession, headers={"User-Agent": ForwardClient.USER_AGENT}
        )
        return ForwardClient(config=karapace_container.config())


async def test_forward_client_close(forward_client: ForwardClient) -> None:
    await forward_client.close()
    forward_client._forward_client.close.assert_awaited_once()


@pytest.mark.parametrize(
    "testcase",
    [
        ContentTypeTestCase(test_name="application/json", content_type="application/json"),
        ContentTypeTestCase(
            test_name="application/vnd.schemaregistry.v1+json", content_type="application/vnd.schemaregistry.v1+json"
        ),
        ContentTypeTestCase(
            test_name="application/vnd.schemaregistry+json", content_type="application/vnd.schemaregistry+json"
        ),
        ContentTypeTestCase(test_name="application/octet-stream", content_type="application/octet-stream"),
    ],
    ids=str,
)
async def test_forward_request_with_basemodel_response(forward_client: ForwardClient, testcase: ContentTypeTestCase) -> None:
    mock_request = Mock(spec=Request)
    mock_request.method = "GET"
    mock_request.headers = Headers()

    mock_get_func = Mock()
    mock_response_context = AsyncMock
    mock_response = AsyncMock()
    mock_response_context.call_function = lambda _: mock_response
    mock_response.status = 200
    mock_response.text.return_value = '{"number":10,"string":"important"}'
    headers = MutableHeaders()
    headers["Content-Type"] = testcase.content_type
    mock_response.headers = headers

    async def mock_aenter(_) -> Mock:
        return mock_response

    async def mock_aexit(_, __, ___, ____) -> None:
        return

    mock_get_func.__aenter__ = mock_aenter
    mock_get_func.__aexit__ = mock_aexit
    forward_client._forward_client.get.return_value = mock_get_func

    response = await forward_client.forward_request_remote(
        request=mock_request,
        primary_url="test-url",
        response_type=TestResponse,
    )

    assert response == TestResponse(number=10, string="important")


async def test_forward_request_with_integer_list_response(forward_client: ForwardClient) -> None:
    mock_request = Mock(spec=Request)
    mock_request.method = "GET"
    mock_request.headers = Headers()

    mock_get_func = Mock()
    mock_response_context = AsyncMock
    mock_response = AsyncMock()
    mock_response_context.call_function = lambda _: mock_response
    mock_response.status = 200
    mock_response.text.return_value = "[1, 2, 3, 10]"
    headers = MutableHeaders()
    headers["Content-Type"] = "application/json"
    mock_response.headers = headers

    async def mock_aenter(_) -> Mock:
        return mock_response

    async def mock_aexit(_, __, ___, ____) -> None:
        return

    mock_get_func.__aenter__ = mock_aenter
    mock_get_func.__aexit__ = mock_aexit
    forward_client._forward_client.get.return_value = mock_get_func

    response = await forward_client.forward_request_remote(
        request=mock_request,
        primary_url="test-url",
        response_type=list[int],
    )

    assert response == [1, 2, 3, 10]


async def test_forward_request_with_integer_response(forward_client: ForwardClient) -> None:
    mock_request = Mock(spec=Request)
    mock_request.method = "GET"
    mock_request.headers = Headers()

    mock_get_func = Mock()
    mock_response_context = AsyncMock
    mock_response = AsyncMock()
    mock_response_context.call_function = lambda _: mock_response
    mock_response.status = 200
    mock_response.text.return_value = "12"
    headers = MutableHeaders()
    headers["Content-Type"] = "application/json"
    mock_response.headers = headers

    async def mock_aenter(_) -> AsyncMock:
        return mock_response

    async def mock_aexit(_, __, ___, ____) -> None:
        return

    mock_get_func.__aenter__ = mock_aenter
    mock_get_func.__aexit__ = mock_aexit
    forward_client._forward_client.get.return_value = mock_get_func

    response = await forward_client.forward_request_remote(
        request=mock_request,
        primary_url="test-url",
        response_type=int,
    )

    assert response == 12


async def test_forward_request_with_https(karapace_container: KarapaceContainer) -> None:
    https_config = karapace_container.config()
    https_config.advertised_protocol = "https"
    https_config.server_tls_cafile = "test-cafile"

    with (
        patch("karapace.api.forward_client.aiohttp") as mocked_aiohttp,
        patch("karapace.api.forward_client.ssl") as mocked_ssl,
        patch("karapace.api.forward_client.os.path.exists") as mock_exists,
    ):
        mock_exists.return_value = True  # Make the CA file appear to exist
        mocked_aiohttp.ClientSession.return_value = Mock(
            spec=aiohttp.ClientSession, headers={"User-Agent": ForwardClient.USER_AGENT}
        )
        forward_client = ForwardClient(config=https_config)

        mocked_ssl.SSLContext.assert_called_once_with(protocol=mocked_ssl.PROTOCOL_TLS_CLIENT)
        mocked_ssl.SSLContext.return_value.load_verify_locations.assert_called_once_with(cafile="test-cafile")

        mock_request = Mock(spec=Request)
        mock_request.method = "GET"
        mock_request.headers = Headers()

        mock_get_func = AsyncMock()
        mock_response_context = AsyncMock
        mock_response = AsyncMock()
        mock_response_context.call_function = lambda _: mock_response
        mock_response.status = 200
        mock_response.text.return_value = "12"
        headers = MutableHeaders()
        headers["Content-Type"] = "application/json"
        mock_response.headers = headers

        async def mock_aenter(_) -> AsyncMock:
            return mock_response

        async def mock_aexit(_, __, ___, ____) -> None:
            return

        mock_get_func.__aenter__ = mock_aenter
        mock_get_func.__aexit__ = mock_aexit
        forward_client._forward_client.get.return_value = mock_get_func

        response = await forward_client.forward_request_remote(
            request=mock_request,
            primary_url="test-url",
            response_type=int,
        )

        assert response == 12


async def test_forwarded_headers_allowlist(forward_client: ForwardClient) -> None:
    """Only Authorization and Content-Type should be forwarded.

    Hop-by-hop headers (Host, Transfer-Encoding, Content-Length, Connection,
    etc.) must NOT be forwarded — aiohttp sets these correctly for the
    outgoing request.  Forwarding them verbatim caused malformed HTTP
    requests on the primary (see: 5.x header-forwarding regression).
    """
    incoming_headers = MutableHeaders()
    incoming_headers["host"] = "broker-1-48.example.com:8081"
    incoming_headers["transfer-encoding"] = "chunked"
    incoming_headers["content-length"] = "999"
    incoming_headers["connection"] = "keep-alive"
    incoming_headers["keep-alive"] = "timeout=5"
    incoming_headers["content-type"] = "application/vnd.schemaregistry.v1+json"
    incoming_headers["authorization"] = "Bearer test-token"
    incoming_headers["te"] = "trailers"
    incoming_headers["trailer"] = "X-Checksum"
    incoming_headers["upgrade"] = "h2c"
    incoming_headers["x-custom-header"] = "should-not-be-forwarded"

    mock_request = Mock(spec=Request)
    mock_request.method = "POST"
    mock_request.headers = Headers(raw=[(k.encode(), v.encode()) for k, v in incoming_headers.items()])

    mock_post_func = Mock()
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text.return_value = '{"number":1,"string":"ok"}'
    response_headers = MutableHeaders()
    response_headers["Content-Type"] = "application/json"
    mock_response.headers = response_headers

    async def mock_aenter(_) -> AsyncMock:
        return mock_response

    async def mock_aexit(_, __, ___, ____) -> None:
        return

    mock_post_func.__aenter__ = mock_aenter
    mock_post_func.__aexit__ = mock_aexit
    forward_client._forward_client.post.return_value = mock_post_func

    await forward_client.forward_request_remote(
        request=mock_request,
        primary_url="http://broker-1-47.example.com:8081",
        response_type=TestResponse,
    )

    call_kwargs = forward_client._forward_client.post.call_args
    forwarded_headers = call_kwargs[1]["headers"] if "headers" in call_kwargs[1] else call_kwargs[0][1]

    assert "authorization" in {k.lower() for k in forwarded_headers}
    assert "content-type" in {k.lower() for k in forwarded_headers}

    for dangerous_header in (
        "host",
        "transfer-encoding",
        "content-length",
        "connection",
        "keep-alive",
        "te",
        "trailer",
        "upgrade",
    ):
        assert dangerous_header not in {
            k.lower() for k in forwarded_headers
        }, f"Hop-by-hop header '{dangerous_header}' must not be forwarded"

    assert "x-custom-header" not in {k.lower() for k in forwarded_headers}, "Only allowlisted headers should be forwarded"


async def test_forward_request_with_error_response(forward_client: ForwardClient) -> None:
    """Test that error responses (4xx/5xx) are properly handled and not parsed as success responses."""
    mock_request = Mock(spec=Request)

    # Mock _forward_request_remote to return error status and body directly
    # This bypasses the aiohttp complexity and tests the error handling logic
    error_body = '{"error_code": 409, "message": "Incompatible schema, compatibility_mode=BACKWARD. Incompatibilities: n_current_date"}'
    forward_client._forward_request_remote = AsyncMock(return_value=(409, error_body))

    with pytest.raises(HTTPException) as exc_info:
        await forward_client.forward_request_remote(
            request=mock_request,
            primary_url="test-url",
            response_type=TestResponse,
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["error_code"] == 409
    assert "Incompatible schema" in exc_info.value.detail["message"]
