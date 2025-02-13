"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import logging
from unittest.mock import Mock

import pytest
from _pytest.logging import LogCaptureFixture
from aiohttp.client_exceptions import ClientConnectionError
from aiohttp.web import Request

from karapace.core.container import KarapaceContainer
from karapace.kafka_rest_apis.karapace import KarapaceBase
from karapace.statsd import StatsClient
from karapace.rapu import REST_ACCEPT_RE, REST_CONTENT_TYPE_RE, HTTPRequest


async def test_header_get():
    req = HTTPRequest(url="", query="", headers={}, path_for_stats="", method="GET")
    assert "Content-Type" not in req.headers
    for v in ["Content-Type", "content-type", "CONTENT-tYPE", "coNTENT-TYpe"]:
        assert req.get_header(v) == "application/json"


def test_rest_accept_re():
    # incomplete headers
    assert not REST_ACCEPT_RE.match("")
    assert not REST_ACCEPT_RE.match("application/")
    assert not REST_ACCEPT_RE.match("application/vnd.kafka")
    assert not REST_ACCEPT_RE.match("application/vnd.kafka.json")
    # Unsupported serialization formats
    assert not REST_ACCEPT_RE.match("application/vnd.kafka+avro")
    assert not REST_ACCEPT_RE.match("application/vnd.kafka+protobuf")
    assert not REST_ACCEPT_RE.match("application/vnd.kafka+binary")

    assert REST_ACCEPT_RE.match("application/json").groupdict() == {
        "embedded_format": None,
        "api_version": None,
        "serialization_format": None,
        "general_format": "json",
    }
    assert REST_ACCEPT_RE.match("application/*").groupdict() == {
        "embedded_format": None,
        "api_version": None,
        "serialization_format": None,
        "general_format": "*",
    }
    assert REST_ACCEPT_RE.match("application/vnd.kafka+json").groupdict() == {
        "embedded_format": None,
        "api_version": None,
        "serialization_format": "json",
        "general_format": None,
    }

    # Embdded format
    assert REST_ACCEPT_RE.match("application/vnd.kafka.avro+json").groupdict() == {
        "embedded_format": "avro",
        "api_version": None,
        "serialization_format": "json",
        "general_format": None,
    }
    assert REST_ACCEPT_RE.match("application/vnd.kafka.json+json").groupdict() == {
        "embedded_format": "json",
        "api_version": None,
        "serialization_format": "json",
        "general_format": None,
    }
    assert REST_ACCEPT_RE.match("application/vnd.kafka.binary+json").groupdict() == {
        "embedded_format": "binary",
        "api_version": None,
        "serialization_format": "json",
        "general_format": None,
    }
    assert REST_ACCEPT_RE.match("application/vnd.kafka.jsonschema+json").groupdict() == {
        "embedded_format": "jsonschema",
        "api_version": None,
        "serialization_format": "json",
        "general_format": None,
    }

    # API version
    assert REST_ACCEPT_RE.match("application/vnd.kafka.v1+json").groupdict() == {
        "embedded_format": None,
        "api_version": "v1",
        "serialization_format": "json",
        "general_format": None,
    }
    assert REST_ACCEPT_RE.match("application/vnd.kafka.v2+json").groupdict() == {
        "embedded_format": None,
        "api_version": "v2",
        "serialization_format": "json",
        "general_format": None,
    }


def test_content_type_re():
    # incomplete headers
    assert not REST_CONTENT_TYPE_RE.match("")
    assert not REST_CONTENT_TYPE_RE.match("application/")
    assert not REST_CONTENT_TYPE_RE.match("application/vnd.kafka")
    assert not REST_CONTENT_TYPE_RE.match("application/vnd.kafka.json")
    # Unsupported serialization formats
    assert not REST_CONTENT_TYPE_RE.match("application/vnd.kafka+avro")
    assert not REST_CONTENT_TYPE_RE.match("application/vnd.kafka+protobuf")
    assert not REST_CONTENT_TYPE_RE.match("application/vnd.kafka+binary")
    # Unspecified format
    assert not REST_CONTENT_TYPE_RE.match("application/*")

    assert REST_CONTENT_TYPE_RE.match("application/json").groupdict() == {
        "embedded_format": None,
        "api_version": None,
        "serialization_format": None,
        "general_format": "json",
    }
    assert REST_CONTENT_TYPE_RE.match("application/octet-stream").groupdict() == {
        "embedded_format": None,
        "api_version": None,
        "serialization_format": None,
        "general_format": "octet-stream",
    }
    assert REST_CONTENT_TYPE_RE.match("application/vnd.kafka+json").groupdict() == {
        "embedded_format": None,
        "api_version": None,
        "serialization_format": "json",
        "general_format": None,
    }

    # Embdded format
    assert REST_CONTENT_TYPE_RE.match("application/vnd.kafka.avro+json").groupdict() == {
        "embedded_format": "avro",
        "api_version": None,
        "serialization_format": "json",
        "general_format": None,
    }
    assert REST_CONTENT_TYPE_RE.match("application/vnd.kafka.json+json").groupdict() == {
        "embedded_format": "json",
        "api_version": None,
        "serialization_format": "json",
        "general_format": None,
    }
    assert REST_CONTENT_TYPE_RE.match("application/vnd.kafka.binary+json").groupdict() == {
        "embedded_format": "binary",
        "api_version": None,
        "serialization_format": "json",
        "general_format": None,
    }
    assert REST_CONTENT_TYPE_RE.match("application/vnd.kafka.jsonschema+json").groupdict() == {
        "embedded_format": "jsonschema",
        "api_version": None,
        "serialization_format": "json",
        "general_format": None,
    }

    # API version
    assert REST_CONTENT_TYPE_RE.match("application/vnd.kafka.v1+json").groupdict() == {
        "embedded_format": None,
        "api_version": "v1",
        "serialization_format": "json",
        "general_format": None,
    }
    assert REST_CONTENT_TYPE_RE.match("application/vnd.kafka.v2+json").groupdict() == {
        "embedded_format": None,
        "api_version": "v2",
        "serialization_format": "json",
        "general_format": None,
    }


@pytest.mark.parametrize("connection_error", (ConnectionError(), ClientConnectionError()))
async def test_raise_connection_error_handling(
    karapace_container: KarapaceContainer, connection_error: BaseException
) -> None:
    request_mock = Mock(spec=Request)
    request_mock.read.side_effect = connection_error
    callback_mock = Mock()

    app = KarapaceBase(config=karapace_container.config())

    response = await app._handle_request(
        request=request_mock,
        path_for_stats="/",
        callback=callback_mock,
    )

    assert response.status == 503
    request_mock.read.assert_has_calls([])
    callback_mock.assert_not_called()


async def test_close_by_app(caplog: LogCaptureFixture, karapace_container: KarapaceContainer) -> None:
    app = KarapaceBase(config=karapace_container.config())
    app.stats = Mock(spec=StatsClient)

    with caplog.at_level(logging.WARNING, logger="karapace.rapu"):
        await app.close_by_app(app=app.app)

    app.stats.increase.assert_called_once_with("karapace_shutdown_count")
    app.stats.close.assert_called_once()
    for log in caplog.records:
        assert log.name == "karapace"
        assert log.levelname == "WARNING"
        assert log.message == "=======> Received shutdown signal, closing Application <======="
