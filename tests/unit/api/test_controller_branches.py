"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from fastapi import HTTPException
from karapace.api.controller import KarapaceSchemaRegistryController
from karapace.api.routers.errors import SchemaErrorCodes
from karapace.api.routers.requests import CompatibilityRequest, SchemaRequest
from karapace.core.config import Config
from karapace.core.errors import (
    IncompatibleSchema,
    ReferenceExistsException,
    SchemasNotFoundException,
    SchemaTooLargeException,
    SchemaVersionNotSoftDeletedException,
    SchemaVersionSoftDeletedException,
    SubjectNotFoundException,
    SubjectNotSoftDeletedException,
    SubjectSoftDeletedException,
    VersionNotFoundException,
)
from karapace.core.schema_models import SchemaType
from karapace.core.stats import StatsClient
from karapace.core.typing import PrimaryInfo, Subject, Version
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest


def _controller(registry_mock: Mock | None = None) -> KarapaceSchemaRegistryController:
    config = Config()
    registry = registry_mock or MagicMock()
    stats = Mock(spec=StatsClient)
    return KarapaceSchemaRegistryController(config=config, schema_registry=registry, stats=stats)


# ---- _invalid_version helper ----


def test_invalid_version_builds_422_exception() -> None:
    ctrl = _controller()

    exc = ctrl._invalid_version("bogus")

    assert exc.status_code == 422
    assert exc.detail["error_code"] == SchemaErrorCodes.INVALID_VERSION_ID.value
    assert "bogus" in exc.detail["message"]
    assert "latest" in exc.detail["message"]


# ---- _validate_schema_type ----


def test_validate_schema_type_rejects_non_dict() -> None:
    ctrl = _controller()

    with pytest.raises(HTTPException) as exc_info:
        ctrl._validate_schema_type(data="not a dict")  # type: ignore[arg-type]

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["error_code"] == SchemaErrorCodes.HTTP_BAD_REQUEST.value


def test_validate_schema_type_defaults_to_avro() -> None:
    ctrl = _controller()

    schema_type = ctrl._validate_schema_type({"schema": "{}"})

    assert schema_type is SchemaType.AVRO


def test_validate_schema_type_accepts_known_types() -> None:
    ctrl = _controller()

    assert ctrl._validate_schema_type({"schemaType": "JSON"}) is SchemaType.JSONSCHEMA
    assert ctrl._validate_schema_type({"schemaType": "PROTOBUF"}) is SchemaType.PROTOBUF


def test_validate_schema_type_rejects_unknown() -> None:
    ctrl = _controller()

    with pytest.raises(HTTPException) as exc_info:
        ctrl._validate_schema_type({"schemaType": "XML"})

    assert exc_info.value.status_code == 422
    assert "XML" in exc_info.value.detail["message"]


# ---- _validate_references ----


def _schema_request(**kwargs) -> SchemaRequest:
    defaults = {"schema": "{}", "schemaType": SchemaType.AVRO.value}
    defaults.update(kwargs)
    return SchemaRequest.model_validate(defaults)


def test_validate_references_returns_none_for_no_references() -> None:
    ctrl = _controller()

    result = ctrl._validate_references(_schema_request())

    assert result is None


def test_validate_references_returns_none_for_empty_list() -> None:
    ctrl = _controller()

    result = ctrl._validate_references(_schema_request(references=[]))

    assert result is None


def test_validate_references_rejects_json_schema_refs() -> None:
    ctrl = _controller()
    req = _schema_request(
        schemaType=SchemaType.JSONSCHEMA.value,
        schema='{"type":"object"}',
        references=[{"name": "r", "subject": "sub", "version": 1}],
    )

    with pytest.raises(HTTPException) as exc_info:
        ctrl._validate_references(req)

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail["error_code"] == SchemaErrorCodes.REFERENCES_SUPPORT_NOT_IMPLEMENTED.value


def test_validate_references_returns_parsed_references() -> None:
    ctrl = _controller()
    req = _schema_request(
        schemaType=SchemaType.PROTOBUF.value,
        schema='syntax = "proto3"; message M {}',
        references=[
            {"name": "r1", "subject": "s1", "version": 1},
            {"name": "r2", "subject": "s2", "version": -1},  # latest
        ],
    )

    result = ctrl._validate_references(req)

    assert result is not None
    assert len(result) == 2
    assert result[0].name == "r1"
    assert result[1].name == "r2"


# ---- _subject_get ----


def test_subject_get_translates_not_found() -> None:
    registry = MagicMock()
    registry.subject_get.side_effect = SubjectNotFoundException()
    ctrl = _controller(registry)

    with pytest.raises(HTTPException) as exc_info:
        ctrl._subject_get(Subject("missing"))

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail["error_code"] == SchemaErrorCodes.SUBJECT_NOT_FOUND.value


def test_subject_get_translates_schemas_not_found() -> None:
    registry = MagicMock()
    registry.subject_get.side_effect = SchemasNotFoundException()
    ctrl = _controller(registry)

    with pytest.raises(HTTPException) as exc_info:
        ctrl._subject_get(Subject("empty"))

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail["error_code"] == SchemaErrorCodes.SUBJECT_NOT_FOUND.value


# ---- schemas_types ----


async def test_schemas_types_returns_supported_types() -> None:
    ctrl = _controller()

    types = await ctrl.schemas_types()

    assert set(types) == {"JSON", "AVRO", "PROTOBUF"}


# ---- config_set ----


async def test_config_set_rejects_invalid_compatibility() -> None:
    ctrl = _controller()

    with pytest.raises(HTTPException) as exc_info:
        await ctrl.config_set(compatibility_level_request=CompatibilityRequest(compatibility="WEIRD"))

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail["error_code"] == SchemaErrorCodes.INVALID_COMPATIBILITY_LEVEL.value


async def test_config_set_accepts_known_level() -> None:
    registry = MagicMock()
    type(registry.schema_reader).config = Mock()
    registry.schema_reader.config.compatibility = "BACKWARD"
    ctrl = _controller(registry)

    response = await ctrl.config_set(compatibility_level_request=CompatibilityRequest(compatibility="BACKWARD"))

    assert response.compatibility == "BACKWARD"
    registry.send_config_message.assert_called_once()


# ---- config_subject_set ----


async def test_config_subject_set_rejects_invalid_level() -> None:
    ctrl = _controller()

    with pytest.raises(HTTPException) as exc_info:
        await ctrl.config_subject_set(subject="s", compatibility_level_request=CompatibilityRequest(compatibility="BOGUS"))

    assert exc_info.value.status_code == 422


async def test_config_subject_set_sends_message_and_returns() -> None:
    registry = MagicMock()
    ctrl = _controller(registry)

    response = await ctrl.config_subject_set(
        subject="s", compatibility_level_request=CompatibilityRequest(compatibility="FORWARD")
    )

    assert response.compatibility == "FORWARD"
    registry.send_config_message.assert_called_once()
    args = registry.send_config_message.call_args
    assert args.kwargs["subject"] == Subject("s")


# ---- config_subject_delete ----


async def test_config_subject_delete_calls_registry() -> None:
    registry = MagicMock()
    registry.schema_reader.config.compatibility = "BACKWARD"
    ctrl = _controller(registry)

    response = await ctrl.config_subject_delete(subject="s")

    assert response.compatibility == "BACKWARD"
    registry.send_config_subject_delete_message.assert_called_once_with(subject=Subject("s"))


# ---- config_subject_get ----


async def test_config_subject_get_not_found_when_no_subject() -> None:
    registry = MagicMock()
    registry.database.find_subject.return_value = None
    ctrl = _controller(registry)

    with pytest.raises(HTTPException) as exc_info:
        await ctrl.config_subject_get(subject="missing", default_to_global=False)

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail["error_code"] == SchemaErrorCodes.SUBJECT_NOT_FOUND.value


async def test_config_subject_get_returns_subject_level() -> None:
    registry = MagicMock()
    registry.database.find_subject.return_value = object()
    registry.database.get_subject_compatibility.return_value = "FULL"
    ctrl = _controller(registry)

    resp = await ctrl.config_subject_get(subject="s", default_to_global=False)

    assert resp.compatibility_level == "FULL"


async def test_config_subject_get_falls_back_to_global() -> None:
    registry = MagicMock()
    registry.database.find_subject.return_value = object()
    registry.database.get_subject_compatibility.return_value = None
    registry.compatibility = "BACKWARD"
    ctrl = _controller(registry)

    resp = await ctrl.config_subject_get(subject="s", default_to_global=True)

    assert resp.compatibility_level == "BACKWARD"


async def test_config_subject_get_404_when_not_configured_and_no_default() -> None:
    registry = MagicMock()
    registry.database.find_subject.return_value = object()
    registry.database.get_subject_compatibility.return_value = None
    ctrl = _controller(registry)

    with pytest.raises(HTTPException) as exc_info:
        await ctrl.config_subject_get(subject="s", default_to_global=False)

    assert exc_info.value.status_code == 404
    assert (
        exc_info.value.detail["error_code"] == SchemaErrorCodes.SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_ERROR_CODE.value
    )


# ---- subject_delete error mapping ----


@pytest.mark.parametrize(
    "exc_cls,expected_status,expected_code",
    [
        (SubjectNotFoundException, 404, SchemaErrorCodes.SUBJECT_NOT_FOUND.value),
        (SchemasNotFoundException, 404, SchemaErrorCodes.SUBJECT_NOT_FOUND.value),
        (SubjectNotSoftDeletedException, 404, SchemaErrorCodes.SUBJECT_NOT_SOFT_DELETED.value),
        (SubjectSoftDeletedException, 404, SchemaErrorCodes.SUBJECT_SOFT_DELETED.value),
    ],
)
async def test_subject_delete_translates_errors(exc_cls, expected_status, expected_code) -> None:
    registry = MagicMock()
    registry.subject_delete_local = AsyncMock(side_effect=exc_cls())
    ctrl = _controller(registry)

    with pytest.raises(HTTPException) as exc_info:
        await ctrl.subject_delete(subject="s", permanent=False)

    assert exc_info.value.status_code == expected_status
    assert exc_info.value.detail["error_code"] == expected_code


async def test_subject_delete_reference_exists_is_422() -> None:
    registry = MagicMock()
    registry.subject_delete_local = AsyncMock(side_effect=ReferenceExistsException([], Version(2)))
    ctrl = _controller(registry)

    with pytest.raises(HTTPException) as exc_info:
        await ctrl.subject_delete(subject="s", permanent=False)

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail["error_code"] == SchemaErrorCodes.REFERENCE_EXISTS.value


async def test_subject_delete_returns_version_list_on_success() -> None:
    registry = MagicMock()
    registry.subject_delete_local = AsyncMock(return_value=[Version(1), Version(2)])
    ctrl = _controller(registry)

    result = await ctrl.subject_delete(subject="s", permanent=False)

    assert result == [1, 2]


# ---- subject_version_get error mapping ----


async def test_subject_version_get_subject_not_found() -> None:
    registry = MagicMock()
    registry.subject_version_get.side_effect = SubjectNotFoundException()
    ctrl = _controller(registry)

    with pytest.raises(HTTPException) as exc_info:
        await ctrl.subject_version_get(subject="x", version="1", deleted=False)

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail["error_code"] == SchemaErrorCodes.SUBJECT_NOT_FOUND.value


async def test_subject_version_get_version_not_found() -> None:
    registry = MagicMock()
    registry.subject_version_get.side_effect = VersionNotFoundException()
    ctrl = _controller(registry)

    with pytest.raises(HTTPException) as exc_info:
        await ctrl.subject_version_get(subject="x", version="99", deleted=False)

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail["error_code"] == SchemaErrorCodes.VERSION_NOT_FOUND.value


async def test_subject_version_get_returns_response() -> None:
    registry = MagicMock()
    registry.subject_version_get.return_value = {
        "subject": "s",
        "version": 1,
        "id": 10,
        "schema": "{}",
    }
    ctrl = _controller(registry)

    resp = await ctrl.subject_version_get(subject="s", version="1", deleted=False)

    assert resp.subject == "s"
    assert resp.version == 1
    assert resp.schema_id == 10


# ---- subject_version_delete error mapping ----


@pytest.mark.parametrize(
    "exc_cls,expected_code",
    [
        (SubjectNotFoundException, SchemaErrorCodes.SUBJECT_NOT_FOUND.value),
        (SchemasNotFoundException, SchemaErrorCodes.SUBJECT_NOT_FOUND.value),
        (VersionNotFoundException, SchemaErrorCodes.VERSION_NOT_FOUND.value),
        (SchemaVersionSoftDeletedException, SchemaErrorCodes.SCHEMAVERSION_SOFT_DELETED.value),
        (SchemaVersionNotSoftDeletedException, SchemaErrorCodes.SCHEMAVERSION_NOT_SOFT_DELETED.value),
    ],
)
async def test_subject_version_delete_translates_errors(exc_cls, expected_code) -> None:
    registry = MagicMock()
    registry.subject_version_delete_local = AsyncMock(side_effect=exc_cls())
    ctrl = _controller(registry)

    with pytest.raises(HTTPException) as exc_info:
        await ctrl.subject_version_delete(subject="s", version="1", permanent=False)

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail["error_code"] == expected_code


async def test_subject_version_delete_returns_resolved_version() -> None:
    registry = MagicMock()
    registry.subject_version_delete_local = AsyncMock(return_value=Version(3))
    ctrl = _controller(registry)

    result = await ctrl.subject_version_delete(subject="s", version="3", permanent=True)

    assert result == 3


# ---- subject_versions_list ----


async def test_subject_versions_list_returns_values() -> None:
    registry = MagicMock()
    registry.subject_get.return_value = {Version(1): object(), Version(2): object()}
    ctrl = _controller(registry)

    result = await ctrl.subject_versions_list(subject="s", deleted=False)

    assert sorted(result) == [1, 2]


async def test_subject_versions_list_not_found() -> None:
    registry = MagicMock()
    registry.subject_get.side_effect = SubjectNotFoundException()
    ctrl = _controller(registry)

    with pytest.raises(HTTPException) as exc_info:
        await ctrl.subject_versions_list(subject="s", deleted=False)

    assert exc_info.value.status_code == 404


# ---- schemas_get (ValueError on schema_id) ----


async def test_schemas_get_non_int_id_returns_404() -> None:
    ctrl = _controller()

    with pytest.raises(HTTPException) as exc_info:
        await ctrl.schemas_get(
            schema_id="not-an-int",
            fetch_max_id=False,
            include_subjects=False,
            format_serialized="",
            user=None,
            authorizer=None,
        )

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail["error_code"] == SchemaErrorCodes.HTTP_NOT_FOUND.value


async def test_schemas_get_missing_schema_returns_404() -> None:
    registry = MagicMock()
    registry.schemas_get.return_value = None
    ctrl = _controller(registry)

    with pytest.raises(HTTPException) as exc_info:
        await ctrl.schemas_get(
            schema_id="123",
            fetch_max_id=False,
            include_subjects=False,
            format_serialized="",
            user=None,
            authorizer=None,
        )

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail["error_code"] == SchemaErrorCodes.SCHEMA_NOT_FOUND.value


# ---- schemas_get_versions ----


async def test_schemas_get_versions_non_int_id_returns_404() -> None:
    ctrl = _controller()

    with pytest.raises(HTTPException) as exc_info:
        await ctrl.schemas_get_versions(schema_id="not-an-int", deleted=False, user=None, authorizer=None)

    assert exc_info.value.status_code == 404


async def test_schemas_get_versions_returns_list() -> None:
    registry = MagicMock()
    registry.get_subject_versions_for_schema.return_value = [
        {"subject": Subject("s1"), "version": Version(1)},
        {"subject": Subject("s2"), "version": Version(3)},
    ]
    ctrl = _controller(registry)

    result = await ctrl.schemas_get_versions(schema_id="42", deleted=False, user=None, authorizer=None)

    assert [sv.subject for sv in result] == [Subject("s1"), Subject("s2")]
    assert [sv.version for sv in result] == [1, 3]


# ---- compatibility_check: ValueError on mode ----


async def test_compatibility_check_internal_error_on_invalid_mode() -> None:
    registry = MagicMock()
    registry.get_compatibility_mode.side_effect = ValueError("bad config")
    ctrl = _controller(registry)

    with pytest.raises(HTTPException) as exc_info:
        await ctrl.compatibility_check(
            subject=Subject("s"),
            schema_request=_schema_request(),
            version="1",
        )

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail["error_code"] == SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value


# ---- get_subject_mode ----


async def test_get_subject_mode_not_found() -> None:
    registry = MagicMock()
    registry.database.find_subject.return_value = None
    ctrl = _controller(registry)

    with pytest.raises(HTTPException) as exc_info:
        await ctrl.get_subject_mode(subject="missing")

    assert exc_info.value.status_code == 404


async def test_get_subject_mode_returns_mode() -> None:
    registry = MagicMock()
    registry.database.find_subject.return_value = object()
    registry.get_global_mode.return_value = "READWRITE"
    ctrl = _controller(registry)

    resp = await ctrl.get_subject_mode(subject="s")

    assert resp.mode == "READWRITE"


async def test_get_global_mode_returns_mode() -> None:
    registry = MagicMock()
    registry.get_global_mode.return_value = "READONLY"
    ctrl = _controller(registry)

    resp = await ctrl.get_global_mode()

    assert resp.mode == "READONLY"


# ---- subject_post error paths (happy-path forwarding is too wide for unit) ----


async def test_subject_post_incompatible_schema_returns_409() -> None:
    registry = MagicMock()
    registry.resolve_references.return_value = (None, None)
    registry.database.get_schema_id_if_exists.return_value = None
    registry.get_master = AsyncMock(return_value=PrimaryInfo(primary=True, primary_url=None))
    registry.write_new_schema_local = AsyncMock(side_effect=IncompatibleSchema("not compat"))
    ctrl = _controller(registry)

    with pytest.raises(HTTPException) as exc_info:
        await ctrl.subject_post(
            subject="s",
            schema_request=_schema_request(
                schema='{"type":"record","name":"R","fields":[]}',
            ),
            normalize=False,
            forward_client=Mock(),
            request=Mock(),
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["error_code"] == SchemaErrorCodes.HTTP_CONFLICT.value


async def test_subject_post_schema_too_large_returns_422() -> None:
    registry = MagicMock()
    registry.resolve_references.return_value = (None, None)
    registry.database.get_schema_id_if_exists.return_value = None
    registry.get_master = AsyncMock(return_value=PrimaryInfo(primary=True, primary_url=None))
    registry.write_new_schema_local = AsyncMock(side_effect=SchemaTooLargeException())
    ctrl = _controller(registry)

    with pytest.raises(HTTPException) as exc_info:
        await ctrl.subject_post(
            subject="s",
            schema_request=_schema_request(
                schema='{"type":"record","name":"R","fields":[]}',
            ),
            normalize=False,
            forward_client=Mock(),
            request=Mock(),
        )

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail["error_code"] == SchemaErrorCodes.SCHEMA_TOO_LARGE_ERROR_CODE.value


async def test_subject_post_returns_existing_schema_id() -> None:
    registry = MagicMock()
    registry.resolve_references.return_value = (None, None)
    registry.database.get_schema_id_if_exists.return_value = 7
    ctrl = _controller(registry)

    resp = await ctrl.subject_post(
        subject="s",
        schema_request=_schema_request(
            schema='{"type":"record","name":"R","fields":[]}',
        ),
        normalize=False,
        forward_client=Mock(),
        request=Mock(),
    )

    assert resp.schema_id == 7
