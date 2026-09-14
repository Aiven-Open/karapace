"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details

Unit tests for surfacing Schema Registry auth failures from the REST proxy as precise
401/403 responses instead of the generic schema-retrieval error (40801).
"""

from __future__ import annotations

from http import HTTPStatus

import pytest

from karapace.core.serialization import SchemaRetrievalError
from karapace.kafka_rest_apis import _raise_for_registry_auth_error
from karapace.kafka_rest_apis.error_codes import RESTErrorCodes
from karapace.rapu import HTTPResponse

_CONTENT_TYPE = "application/vnd.kafka.avro.v2+json"


class TestSchemaRetrievalError:
    def test_status_code_defaults_to_none(self) -> None:
        assert SchemaRetrievalError("boom").status_code is None

    def test_status_code_is_preserved(self) -> None:
        assert SchemaRetrievalError("nope", status_code=401).status_code == 401


class TestRaiseForRegistryAuthError:
    def test_unauthorized_raises_precise_401(self) -> None:
        with pytest.raises(HTTPResponse) as exc_info:
            _raise_for_registry_auth_error(SchemaRetrievalError("x", status_code=401), _CONTENT_TYPE)
        resp = exc_info.value
        assert resp.status == HTTPStatus.UNAUTHORIZED
        assert resp.body["error_code"] == RESTErrorCodes.HTTP_UNAUTHORIZED.value

    def test_forbidden_raises_precise_403(self) -> None:
        with pytest.raises(HTTPResponse) as exc_info:
            _raise_for_registry_auth_error(SchemaRetrievalError("x", status_code=403), _CONTENT_TYPE)
        resp = exc_info.value
        assert resp.status == HTTPStatus.FORBIDDEN
        assert resp.body["error_code"] == RESTErrorCodes.HTTP_FORBIDDEN.value

    @pytest.mark.parametrize("status_code", [None, 404, 409, 422, 500])
    def test_non_auth_status_is_noop(self, status_code: int | None) -> None:
        # Must NOT raise: the caller falls through to the generic schema-retrieval error.
        _raise_for_registry_auth_error(SchemaRetrievalError("x", status_code=status_code), _CONTENT_TYPE)
