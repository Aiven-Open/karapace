"""
karapace - master forwarding tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from dataclasses import dataclass
from fastapi import Request
from fastapi.datastructures import Headers
from karapace.forward_client import ForwardClient
from pydantic import BaseModel
from starlette.datastructures import MutableHeaders
from tests.base_testcase import BaseTestCase
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import pytest


class TestResponse(BaseModel):
    number: int
    string: str


@dataclass
class ContentTypeTestCase(BaseTestCase):
    content_type: str


@pytest.fixture(name="forward_client")
def fixture_forward_client() -> ForwardClient:
    with patch("karapace.forward_client.aiohttp") as mocked_aiohttp:
        mocked_aiohttp.ClientSession.return_value = Mock(
            spec=aiohttp.ClientSession, headers={"User-Agent": ForwardClient.USER_AGENT}
        )
        return ForwardClient()


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
    mock_response.text.return_value = "12"
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
        response_type=int,
    )

    assert response == 12
