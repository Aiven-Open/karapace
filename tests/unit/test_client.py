"""
Tests for the core HTTP client (``karapace.core.client``).

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from karapace.core.client import Client, Result


class TestResult:
    @pytest.mark.parametrize(
        ("status", "expected_ok"),
        [
            (200, True),
            (201, True),
            (204, True),
            (299, True),
            (300, False),
            (400, False),
            (404, False),
            (500, False),
        ],
    )
    def test_ok_is_true_only_for_2xx(self, status: int, expected_ok: bool) -> None:
        result = Result(status=status, json_result={})
        assert result.ok is expected_ok


class TestClientConstruction:
    def test_ssl_mode_is_false_when_no_server_ca_is_provided(self) -> None:
        client = Client(server_uri="http://example.com")
        assert client.ssl_mode is False


class TestGetClient:
    async def test_lazily_creates_session_on_first_call(self) -> None:
        fake_session = MagicMock()
        factory = AsyncMock(return_value=fake_session)
        client = Client(server_uri="http://example.com", client_factory=factory)

        assert client._client is None
        session = await client.get_client()
        assert session is fake_session
        factory.assert_awaited_once_with(auth=None)

    async def test_subsequent_calls_reuse_the_same_session(self) -> None:
        fake_session = MagicMock()
        factory = AsyncMock(return_value=fake_session)
        client = Client(server_uri="http://example.com", client_factory=factory)

        first = await client.get_client()
        second = await client.get_client()
        assert first is second
        factory.assert_awaited_once()


class TestClose:
    async def test_close_swallows_exceptions(self) -> None:
        fake_session = MagicMock()
        fake_session.close = AsyncMock(side_effect=RuntimeError("boom"))
        factory = AsyncMock(return_value=fake_session)
        client = Client(server_uri="http://example.com", client_factory=factory)

        await client.get_client()
        # Errors are intentionally suppressed and logged, must not propagate.
        await client.close()


def _make_aiohttp_response(status: int, json_payload: object | None = None, text_payload: str = "") -> MagicMock:
    """Build a mock aiohttp response usable inside ``async with client.<verb>(...)``."""
    response = MagicMock()
    response.status = status
    response.headers = {"X-Test": "1"}
    response.json = AsyncMock(return_value=json_payload)
    response.text = AsyncMock(return_value=text_payload)

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=response)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


class TestHTTPMethods:
    """The non-obvious branches: 204 short-circuiting, default headers, raw text mode."""

    async def test_get_can_return_raw_text_when_json_response_is_false(self) -> None:
        session = MagicMock()
        session.get = MagicMock(return_value=_make_aiohttp_response(200, text_payload="raw body"))
        factory = AsyncMock(return_value=session)
        client = Client(server_uri="http://example.com/", client_factory=factory)

        result = await client.get("schemas/ids/1/schema", json_response=False)
        assert result.status_code == 200
        assert result.json_result == "raw body"

    async def test_post_uses_default_content_type_when_none_provided(self) -> None:
        session = MagicMock()
        session.post = MagicMock(return_value=_make_aiohttp_response(200, {"id": 1}))
        factory = AsyncMock(return_value=session)
        client = Client(server_uri="http://example.com/", client_factory=factory)

        await client.post("subjects/test/versions", json={"schema": "{}"})
        headers = session.post.call_args.kwargs["headers"]
        assert headers == {"Content-Type": "application/vnd.schemaregistry.v1+json"}

    async def test_post_returns_empty_dict_on_204(self) -> None:
        session = MagicMock()
        # When status==204 the client must not invoke res.json() but still
        # produce a Result whose body is an empty dict.
        response_cm = _make_aiohttp_response(204)
        session.post = MagicMock(return_value=response_cm)
        factory = AsyncMock(return_value=session)
        client = Client(server_uri="http://example.com/", client_factory=factory)

        result = await client.post("subjects/test", json={})
        assert result.status_code == 204
        assert result.json_result == {}
        # res.json should not be called when we shortcut on 204.
        response_cm.__aenter__.return_value.json.assert_not_called()

    async def test_delete_returns_empty_dict_on_204(self) -> None:
        session = MagicMock()
        response_cm = _make_aiohttp_response(204)
        session.delete = MagicMock(return_value=response_cm)
        factory = AsyncMock(return_value=session)
        client = Client(server_uri="http://example.com/", client_factory=factory)

        result = await client.delete("subjects/test")
        assert result.status_code == 204
        assert result.json_result == {}

    async def test_put_with_data_passes_raw_body_not_json(self) -> None:
        session = MagicMock()
        session.put = MagicMock(return_value=_make_aiohttp_response(200, {"ok": True}))
        factory = AsyncMock(return_value=session)
        client = Client(server_uri="http://example.com/", client_factory=factory)

        await client.put_with_data("some/path", data=b"raw-bytes", headers={"Content-Type": "application/octet-stream"})
        assert session.put.call_args.kwargs["data"] == b"raw-bytes"
        # `put_with_data` must NOT serialise as json.
        assert "json" not in session.put.call_args.kwargs


class TestResourceHelpers:
    """Path building for the per-resource helpers — quoting and query strings matter."""

    async def test_get_subjects_versions_url_encodes_subject(self) -> None:
        client = Client(server_uri="http://example.com/")
        with patch.object(client, "get", new=AsyncMock(return_value=Result(200, [1]))) as mocked_get:
            await client.get_subjects_versions(subject="my topic-value")
            # The space must be URL-encoded to keep the path valid.
            assert mocked_get.await_args is not None
            assert mocked_get.await_args.args[0] == "subjects/my+topic-value/versions"

    async def test_delete_subjects_version_appends_permanent_query(self) -> None:
        client = Client(server_uri="http://example.com/")
        with patch.object(client, "delete", new=AsyncMock(return_value=Result(200, 1))) as mocked_delete:
            await client.delete_subjects_version(subject="t", version=1, permanent=True)
            assert mocked_delete.await_args is not None
            assert mocked_delete.await_args.args[0] == "subjects/t/versions/1?permanent=true"

    async def test_get_config_subject_appends_default_to_global_query(self) -> None:
        client = Client(server_uri="http://example.com/")
        with patch.object(client, "get", new=AsyncMock(return_value=Result(200, {}))) as mocked_get:
            await client.get_config_subject(subject="t", defaultToGlobal=True)
            assert mocked_get.await_args is not None
            assert mocked_get.await_args.kwargs["path"] == "/config/t?defaultToGlobal=true"
