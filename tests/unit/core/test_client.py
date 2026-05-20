"""
Tests for the core HTTP client (``karapace.core.client``).

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import ssl
from collections.abc import Awaitable, Callable
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import BasicAuth

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

    def test_ssl_mode_loads_ca_when_server_ca_set(self, tmp_path) -> None:
        # The CA file content doesn't matter for the branch under test —
        # we only need ``load_verify_locations`` to be invoked with the right path.
        ca_path = tmp_path / "ca.pem"
        ca_path.write_text("placeholder")

        with patch.object(ssl.SSLContext, "load_verify_locations") as mock_load:
            client = Client(server_uri="https://example.com", server_ca=str(ca_path))

        assert isinstance(client.ssl_mode, ssl.SSLContext)
        mock_load.assert_called_once_with(cafile=str(ca_path))


class TestPathFor:
    """Pin ``path_for`` behaviour to prevent the classic ``urljoin`` foot-gun."""

    @pytest.mark.parametrize(
        ("server_uri", "path", "expected"),
        [
            # Bare host (no trailing slash) still resolves the relative path
            # against the host root.
            ("http://h", "subjects", "http://h/subjects"),
            ("http://h", "/subjects", "http://h/subjects"),
            ("http://h/", "subjects", "http://h/subjects"),
            ("http://h/", "/subjects", "http://h/subjects"),
            # With a base path: a relative path is appended, but an absolute
            # path (leading "/") resets back to the host root — the classic
            # urljoin foot-gun worth pinning.
            ("http://h/api/", "subjects", "http://h/api/subjects"),
            ("http://h/api/", "/subjects", "http://h/subjects"),
            # Empty server_uri keeps the path as-is.
            ("", "/subjects", "/subjects"),
        ],
    )
    def test_path_for(self, server_uri: str, path: str, expected: str) -> None:
        client = Client(server_uri=server_uri)
        assert client.path_for(path) == expected


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

    async def test_factory_is_called_with_session_auth(self) -> None:
        factory = AsyncMock(return_value=MagicMock())
        auth = BasicAuth(login="u", password="p")
        client = Client(server_uri="http://example.com", client_factory=factory, session_auth=auth)

        await client.get_client()
        factory.assert_awaited_once_with(auth=auth)


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


def _make_client_with_session(verb: str, response_cm: MagicMock) -> tuple[Client, MagicMock]:
    """Build a Client whose lazy session yields ``response_cm`` for the given verb."""
    session = MagicMock()
    setattr(session, verb, MagicMock(return_value=response_cm))
    factory: Callable[..., Awaitable[MagicMock]] = AsyncMock(return_value=session)
    client = Client(server_uri="http://example.com/", client_factory=factory)
    return client, session


class TestHTTPMethods:
    """Happy paths + the non-obvious branches (204 short-circuit, raw text, default headers)."""

    async def test_get_parses_json_body_by_default(self) -> None:
        client, session = _make_client_with_session("get", _make_aiohttp_response(200, {"subjects": ["a"]}))
        result = await client.get("subjects")
        assert result.status_code == 200
        assert result.json() == {"subjects": ["a"]}
        # path_for resolved to the absolute URL, ssl_mode was forwarded as False.
        assert session.get.call_args.args == ("http://example.com/subjects",)
        assert session.get.call_args.kwargs["ssl"] is False

    async def test_get_can_return_raw_text_when_json_response_is_false(self) -> None:
        client, _ = _make_client_with_session("get", _make_aiohttp_response(200, text_payload="raw body"))
        result = await client.get("schemas/ids/1/schema", json_response=False)
        assert result.status_code == 200
        assert result.json_result == "raw body"

    async def test_get_forwards_auth_and_params(self) -> None:
        client, session = _make_client_with_session("get", _make_aiohttp_response(200, {}))
        auth = BasicAuth(login="u", password="p")
        await client.get("subjects", auth=auth, params={"deleted": "true"})

        kwargs = session.get.call_args.kwargs
        assert kwargs["auth"] is auth
        assert kwargs["params"] == {"deleted": "true"}

    async def test_post_uses_default_content_type_when_none_provided(self) -> None:
        client, session = _make_client_with_session("post", _make_aiohttp_response(200, {"id": 1}))
        await client.post("subjects/test/versions", json={"schema": "{}"})
        assert session.post.call_args.kwargs["headers"] == {"Content-Type": "application/vnd.schemaregistry.v1+json"}

    async def test_post_returns_empty_dict_on_204(self) -> None:
        # When status==204 the client must not invoke res.json() but still
        # produce a Result whose body is an empty dict.
        response_cm = _make_aiohttp_response(204)
        client, _ = _make_client_with_session("post", response_cm)

        result = await client.post("subjects/test", json={})
        assert result.status_code == 204
        assert result.json_result == {}
        # res.json should not be called when we shortcut on 204.
        response_cm.__aenter__.return_value.json.assert_not_called()

    async def test_put_parses_json_body(self) -> None:
        client, _ = _make_client_with_session("put", _make_aiohttp_response(200, {"compatibility": "FULL"}))
        result = await client.put("config", json={"compatibility": "FULL"})
        assert result.status_code == 200
        assert result.json() == {"compatibility": "FULL"}

    async def test_delete_returns_empty_dict_on_204(self) -> None:
        client, _ = _make_client_with_session("delete", _make_aiohttp_response(204))
        result = await client.delete("subjects/test")
        assert result.status_code == 204
        assert result.json_result == {}

    async def test_put_with_data_passes_raw_body_not_json(self) -> None:
        client, session = _make_client_with_session("put", _make_aiohttp_response(200, {"ok": True}))
        await client.put_with_data("some/path", data=b"raw-bytes", headers={"Content-Type": "application/octet-stream"})
        assert session.put.call_args.kwargs["data"] == b"raw-bytes"
        # ``put_with_data`` must NOT serialise as json.
        assert "json" not in session.put.call_args.kwargs


class TestResourceHelpers:
    """Path building for the per-resource helpers — quoting and query strings matter."""

    async def test_get_subjects_versions_url_encodes_subject(self) -> None:
        client = Client(server_uri="http://example.com/")
        with patch.object(client, "get", new=AsyncMock(return_value=Result(200, [1]))) as mocked_get:
            await client.get_subjects_versions(subject="my topic-value")
            # NB: ``quote_plus`` encodes ' ' as '+', not '%20' (which is what ``quote`` would emit).
            # A future "fix" to use quote() would break this assertion intentionally.
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

    @pytest.mark.parametrize(
        ("method_name", "kwargs", "underlying", "expected_path_field", "expected_path"),
        [
            (
                "post_compatibility_subject_version",
                {"subject": "t", "version": "latest", "json": {"schema": "{}"}},
                "post",
                "path",
                "compatibility/subjects/t/versions/latest",
            ),
            (
                "put_config_subject",
                {"subject": "t", "json": {"compatibility": "FULL"}},
                "put",
                "path",
                "config/t",
            ),
            (
                "get_subjects_subject_version",
                {"subject": "t", "version": "latest"},
                "get",
                "positional",
                "/subjects/t/versions/latest",
            ),
            (
                "get_schema_by_id",
                {"schema_id": 42},
                "get",
                "path",
                "/schemas/ids/42",
            ),
            (
                "get_mode",
                {},
                "get",
                "path",
                "/mode",
            ),
            (
                "post_subjects",
                {"subject": "t", "json": {"schema": "{}"}},
                "post",
                "positional",
                "/subjects/t",
            ),
        ],
        ids=lambda v: v if isinstance(v, str) else None,
    )
    async def test_resource_helper_builds_expected_path(
        self,
        method_name: str,
        kwargs: dict,
        underlying: str,
        expected_path_field: str,
        expected_path: str,
    ) -> None:
        client = Client(server_uri="http://example.com/")
        with patch.object(client, underlying, new=AsyncMock(return_value=Result(200, {}))) as mocked:
            await getattr(client, method_name)(**kwargs)

        assert mocked.await_args is not None
        # Some helpers pass the path positionally, others as ``path=``.
        if expected_path_field == "positional":
            actual = mocked.await_args.args[0]
        else:
            actual = mocked.await_args.kwargs[expected_path_field]
        assert actual == expected_path
