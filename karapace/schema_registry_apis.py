from contextlib import closing
from enum import Enum, unique
from http import HTTPStatus
from karapace import version as karapace_version
from karapace.avro_compatibility import is_incompatible
from karapace.compatibility import check_compatibility, CompatibilityModes
from karapace.config import read_config
from karapace.karapace import KarapaceBase
from karapace.master_coordinator import MasterCoordinator
from karapace.rapu import HTTPRequest
from karapace.schema_reader import InvalidSchema, KafkaSchemaReader, SchemaType, TypedSchema
from karapace.utils import json_encode
from typing import Any, Dict, Optional

import argparse
import asyncio
import logging
import sys
import time


@unique
class SchemaErrorCodes(Enum):
    HTTP_NOT_FOUND = HTTPStatus.NOT_FOUND.value
    HTTP_CONFLICT = HTTPStatus.CONFLICT.value
    HTTP_UNPROCESSABLE_ENTITY = HTTPStatus.UNPROCESSABLE_ENTITY.value
    HTTP_INTERNAL_SERVER_ERROR = HTTPStatus.INTERNAL_SERVER_ERROR.value
    SUBJECT_NOT_FOUND = 40401
    VERSION_NOT_FOUND = 40402
    SCHEMA_NOT_FOUND = 40403
    SUBJECT_SOFT_DELETED = 40404
    SUBJECT_NOT_SOFT_DELETED = 40405
    SCHEMAVERSION_SOFT_DELETED = 40406
    SCHEMAVERSION_NOT_SOFT_DELETED = 40407
    INVALID_VERSION_ID = 42202
    INVALID_COMPATIBILITY_LEVEL = 42203
    INVALID_AVRO_SCHEMA = 44201
    NO_MASTER_ERROR = 50003


class InvalidSchemaType(Exception):
    pass


class KarapaceSchemaRegistry(KarapaceBase):
    # pylint: disable=attribute-defined-outside-init
    def __init__(self, config_file_path: str, config: dict) -> None:
        super().__init__(config_file_path=config_file_path, config=config)
        self._add_routes()
        self._init(config=config)
        self.schema_lock = asyncio.Lock()

    def _init(self, config: dict) -> None:  # pylint: disable=unused-argument
        self.ksr = None
        self.producer = None
        self.producer = self._create_producer()
        self._create_master_coordinator()
        self._create_schema_reader()

    def _add_routes(self):
        self.route(
            "/compatibility/subjects/<subject:path>/versions/<version:path>",
            callback=self.compatibility_check,
            method="POST",
            schema_request=True
        )
        self.route(
            "/config/<subject:path>",
            callback=self.config_subject_get,
            method="GET",
            schema_request=True,
            with_request=True,
            json_body=False,
        )
        self.route("/config/<subject:path>", callback=self.config_subject_set, method="PUT", schema_request=True)
        self.route("/config", callback=self.config_get, method="GET", schema_request=True)
        self.route("/config", callback=self.config_set, method="PUT", schema_request=True)
        self.route("/schemas/ids/<schema_id:path>", callback=self.schemas_get, method="GET", schema_request=True)
        self.route("/subjects", callback=self.subjects_list, method="GET", schema_request=True)
        self.route("/subjects/<subject:path>/versions", callback=self.subject_post, method="POST", schema_request=True)
        self.route("/subjects/<subject:path>", callback=self.subjects_schema_post, method="POST", schema_request=True)
        self.route(
            "/subjects/<subject:path>/versions", callback=self.subject_versions_list, method="GET", schema_request=True
        )
        self.route(
            "/subjects/<subject:path>/versions/<version>",
            callback=self.subject_version_get,
            method="GET",
            schema_request=True
        )
        self.route(
            "/subjects/<subject:path>/versions/<version:path>",  # needs
            callback=self.subject_version_delete,
            method="DELETE",
            schema_request=True,
            with_request=True,
            json_body=False,
        )
        self.route(
            "/subjects/<subject:path>/versions/<version>/schema",
            callback=self.subject_version_schema_get,
            method="GET",
            schema_request=True
        )
        self.route(
            "/subjects/<subject:path>",
            callback=self.subject_delete,
            method="DELETE",
            schema_request=True,
            with_request=True,
            json_body=False,
        )

    def close(self):
        super().close()
        self.log.info("Shutting down all auxiliary threads")
        if self.mc:
            self.mc.close()
        if self.ksr:
            self.ksr.close()
        if self.producer:
            self.producer.close()

    def _create_schema_reader(self):
        self.ksr = KafkaSchemaReader(config=self.config, master_coordinator=self.mc)
        self.ksr.start()

    def _create_master_coordinator(self):
        self.mc = MasterCoordinator(config=self.config)
        self.mc.start()

    def _subject_get(self, subject, content_type, include_deleted=False) -> Dict[str, Any]:
        subject_data = self.ksr.subjects.get(subject)
        if not subject_data:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": f"Subject '{subject}' not found.",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        schemas = self.ksr.get_schemas(subject, include_deleted=include_deleted)
        if not schemas:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": f"Subject '{subject}' not found.",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        subject_data = subject_data.copy()
        subject_data["schemas"] = schemas
        return subject_data

    def _validate_version(self, content_type, version):  # pylint: disable=inconsistent-return-statements
        try:
            version_number = int(version)
            if version_number > 0:
                return version
        except ValueError:
            if version == "latest":
                return version
        self.r(
            body={
                "error_code": SchemaErrorCodes.INVALID_VERSION_ID.value,
                "message": (
                    "The specified version is not a valid version id. "
                    "Allowed values are between [1, 2^31-1] and the string \"latest\""
                ),
            },
            content_type=content_type,
            status=HTTPStatus.UNPROCESSABLE_ENTITY,
        )

    def _get_compatibility_mode(self, subject, content_type) -> CompatibilityModes:
        compatibility = subject.get("compatibility", self.ksr.config["compatibility"])

        try:
            compatibility_mode = CompatibilityModes(compatibility)
        except ValueError:
            # Using INTERNAL_SERVER_ERROR because the subject and configuration
            # should have been validated before.
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                    "message": f"Unknown compatibility mode {compatibility}",
                },
                content_type=content_type,
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
        return compatibility_mode

    def get_offset_from_queue(self, sent_offset):
        start_time = time.monotonic()
        while True:
            self.log.info("Starting to wait for offset: %r from ksr queue", sent_offset)
            offset = self.ksr.queue.get()
            if offset == sent_offset:
                self.log.info(
                    "We've consumed back produced offset: %r message back, everything is in sync, took: %.4f", offset,
                    time.monotonic() - start_time
                )
                break
            self.log.warning("Put the offset: %r back to queue, someone else is waiting for this?", offset)
            self.ksr.queue.put(offset)

    def send_kafka_message(self, key, value):
        if isinstance(key, str):
            key = key.encode("utf8")
        if isinstance(value, str):
            value = value.encode("utf8")

        future = self.producer.send(self.config["topic_name"], key=key, value=value)
        self.producer.flush(timeout=self.kafka_timeout)
        msg = future.get(self.kafka_timeout)
        self.log.debug("Sent kafka msg key: %r, value: %r, offset: %r", key, value, msg.offset)
        self.get_offset_from_queue(msg.offset)
        return future

    def send_schema_message(
        self,
        *,
        subject: str,
        schema: Optional[TypedSchema],
        schema_id: int,
        version: int,
        deleted: bool,
    ):
        key = '{{"subject":"{}","version":{},"magic":1,"keytype":"SCHEMA"}}'.format(subject, version)
        if schema:
            valuedict = {
                "subject": subject,
                "version": version,
                "id": schema_id,
                "schema": schema.schema_str,
                "deleted": deleted
            }
            if schema.schema_type is not SchemaType.AVRO:
                valuedict["schemaType"] = schema.schema_type
            value = json_encode(valuedict, compact=True)
        else:
            value = ""
        return self.send_kafka_message(key, value)

    def send_config_message(self, compatibility_level: CompatibilityModes, subject=None):
        if subject is not None:
            key = '{{"subject":"{}","magic":0,"keytype":"CONFIG"}}'.format(subject)
        else:
            key = '{"subject":null,"magic":0,"keytype":"CONFIG"}'
        value = '{{"compatibilityLevel":"{}"}}'.format(compatibility_level.value)
        return self.send_kafka_message(key, value)

    def send_delete_subject_message(self, subject, version):
        key = '{{"subject":"{}","magic":0,"keytype":"DELETE_SUBJECT"}}'.format(subject)
        value = '{{"subject":"{}","version":{}}}'.format(subject, version)
        return self.send_kafka_message(key, value)

    async def compatibility_check(self, content_type, *, subject, version, request):
        """Check for schema compatibility"""
        body = request.json
        self.log.info("Got request to check subject: %r, version_id: %r compatibility", subject, version)
        old = await self.subject_version_get(content_type=content_type, subject=subject, version=version, return_dict=True)
        self.log.info("Existing schema: %r, new_schema: %r", old["schema"], body["schema"])
        try:
            schema_type = SchemaType(body.get("schemaType", "AVRO"))
            new_schema = TypedSchema.parse(schema_type, body["schema"])
        except InvalidSchema:
            self.log.warning("Invalid schema: %r", body["schema"])
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_AVRO_SCHEMA.value,
                    "message": "Invalid Avro schema",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )
        try:
            old_schema_type = SchemaType(old.get("schemaType", "AVRO"))
            old_schema = TypedSchema.parse(old_schema_type, old["schema"])
        except InvalidSchema:
            self.log.warning("Invalid existing schema: %r", old["schema"])
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_AVRO_SCHEMA.value,
                    "message": "Invalid Avro schema",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

        compatibility_mode = self._get_compatibility_mode(subject=old, content_type=content_type)

        result = check_compatibility(
            old_schema=old_schema,
            new_schema=new_schema,
            compatibility_mode=compatibility_mode,
        )
        if is_incompatible(result):
            self.log.warning(
                "Invalid schema %s found by compatibility check: old: %s new: %s", result, old_schema, new_schema
            )
            self.r({"is_compatible": False}, content_type)
        self.r({"is_compatible": True}, content_type)

    async def schemas_get(self, content_type, *, schema_id):
        try:
            schema_id_int = int(schema_id)
        except ValueError:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_NOT_FOUND.value,
                    "message": "HTTP 404 Not Found",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        with self.ksr.id_lock:
            schema = self.ksr.schemas.get(schema_id_int)
        if not schema:
            self.log.warning("Schema: %r that was requested, not found", int(schema_id))
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SCHEMA_NOT_FOUND.value,
                    "message": "Schema not found",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        response_body = {"schema": schema.schema_str}
        if schema.schema_type is not SchemaType.AVRO:
            response_body["schemaType"] = schema.schema_type
        self.r(response_body, content_type)

    async def config_get(self, content_type):
        # Note: The format sent by the user differs from the return value, this
        # is for compatibility reasons.
        self.r({"compatibilityLevel": self.ksr.config["compatibility"]}, content_type)

    async def config_set(self, content_type, *, request):
        body = request.json

        try:
            compatibility_level = CompatibilityModes(request.json["compatibility"])
        except (ValueError, KeyError):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_COMPATIBILITY_LEVEL.value,
                    "message": "Invalid compatibility level. Valid values are none, backward, forward and full",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

        are_we_master, master_url = await self.get_master()
        if are_we_master:
            self.send_config_message(compatibility_level=compatibility_level, subject=None)
        elif are_we_master is None:
            self.no_master_error(content_type)
        else:
            url = f"{master_url}/config"
            await self.forward_request_remote(body=body, url=url, content_type=content_type, method="PUT")

        self.r({"compatibility": self.ksr.config["compatibility"]}, content_type)

    async def config_subject_get(self, content_type, subject: str, *, request: HTTPRequest):
        # Config for a subject can exist without schemas so no need to check for their existence
        assert self.ksr, "KarapaceSchemaRegistry not initialized. Missing call to _init"
        subject_data = self.ksr.subjects.get(subject, {})

        if subject_data:
            default_to_global = request.query.get("defaultToGlobal", "false").lower() == "true"
            compatibility = subject_data.get("compatibility")
            if not compatibility and default_to_global:
                compatibility = self.ksr.config["compatibility"]
            if compatibility:
                # Note: The format sent by the user differs from the return
                # value, this is for compatibility reasons.
                self.r(
                    {"compatibilityLevel": compatibility},
                    content_type,
                )

        self.r(
            body={
                "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                "message": "Subject not found.",
            },
            content_type=content_type,
            status=HTTPStatus.NOT_FOUND,
        )

    async def config_subject_set(self, content_type, *, request, subject):
        try:
            compatibility_level = CompatibilityModes(request.json["compatibility"])
        except (ValueError, KeyError):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_COMPATIBILITY_LEVEL.value,
                    "message": "Invalid compatibility level",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

        are_we_master, master_url = await self.get_master()
        if are_we_master:
            self.send_config_message(compatibility_level=compatibility_level, subject=subject)
        elif are_we_master is None:
            self.no_master_error(content_type)
        else:
            url = f"{master_url}/config/{subject}"
            await self.forward_request_remote(body=request.json, url=url, content_type=content_type, method="PUT")

        self.r({"compatibility": compatibility_level.value}, content_type)

    async def subjects_list(self, content_type):
        subjects_list = [key for key, val in self.ksr.subjects.items() if self.ksr.get_schemas(key)]
        self.r(subjects_list, content_type, status=HTTPStatus.OK)

    async def _subject_delete_local(self, content_type: str, subject: str, permanent: bool):
        subject_data = self._subject_get(subject, content_type, include_deleted=permanent)

        if permanent and [version for version, value in subject_data["schemas"].items() if not value.get("deleted", False)]:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_SOFT_DELETED.value,
                    "message": f"Subject '{subject}' was not deleted first before being permanently deleted",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        version_list = list(subject_data["schemas"])
        if version_list:
            latest_schema_id = version_list[-1]
        else:
            latest_schema_id = 0

        if permanent:
            for version, value in list(subject_data["schemas"].items()):
                schema_id = value.get("id")
                self.log.info("Permanently deleting subject '%s' version %s (schema id=%s)", subject, version, schema_id)
                self.send_schema_message(subject=subject, schema=None, schema_id=schema_id, version=version, deleted=True)
        else:
            self.send_delete_subject_message(subject, latest_schema_id)
        self.r(version_list, content_type, status=HTTPStatus.OK)

    async def subject_delete(self, content_type, *, subject, request: HTTPRequest):
        permanent = request.query.get("permanent", "false").lower() == "true"

        are_we_master, master_url = await self.get_master()
        if are_we_master:
            async with self.schema_lock:
                await self._subject_delete_local(content_type, subject, permanent)
        elif are_we_master is None:
            self.no_master_error(content_type)
        else:
            url = f"{master_url}/subjects/{subject}?permanent={permanent}"
            await self.forward_request_remote(body={}, url=url, content_type=content_type, method="DELETE")

    async def subject_version_get(self, content_type, *, subject, version, return_dict=False):
        self._validate_version(content_type, version)
        subject_data = self._subject_get(subject, content_type)
        schema_data = None
        max_version = max(subject_data["schemas"])
        if version == "latest":
            version = max(subject_data["schemas"])
            schema_data = subject_data["schemas"][version]
        elif int(version) <= max_version:
            schema_data = subject_data["schemas"].get(int(version))
        else:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        if not schema_data:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        schema_id = schema_data["id"]
        schema = schema_data["schema"]

        ret = {
            "subject": subject,
            "version": int(version),
            "id": schema_id,
            "schema": schema.schema_str,
        }
        if schema.schema_type is not SchemaType.AVRO:
            ret["schemaType"] = schema.schema_type
        if return_dict:
            # Return also compatibility information to compatibility check
            if subject_data.get("compatibility"):
                ret["compatibility"] = subject_data.get("compatibility")
            return ret
        self.r(ret, content_type)

    async def _subject_version_delete_local(self, content_type: str, subject: str, version: int, permanent: bool):
        subject_data = self._subject_get(subject, content_type, include_deleted=True)

        subject_schema_data = subject_data["schemas"].get(version, None)
        if not subject_schema_data:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        if subject_schema_data.get("deleted", False) and not permanent:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SCHEMAVERSION_SOFT_DELETED.value,
                    "message": f"Subject '{subject}' Version 1 was soft deleted.Set permanent=true to delete permanently",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        # Cannot directly hard delete
        if permanent and not subject_schema_data.get("deleted", False):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SCHEMAVERSION_NOT_SOFT_DELETED.value,
                    "message": (
                        f"Subject '{subject}' Version {version} was not deleted "
                        "first before being permanently deleted"
                    ),
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        schema_id = subject_schema_data["id"]
        schema = subject_schema_data["schema"]
        self.send_schema_message(
            subject=subject, schema=None if permanent else schema, schema_id=schema_id, version=version, deleted=True
        )
        self.r(str(version), content_type, status=HTTPStatus.OK)

    async def subject_version_delete(self, content_type, *, subject, version, request: HTTPRequest):
        version = int(version)
        permanent = request.query.get("permanent", "false").lower() == "true"

        are_we_master, master_url = await self.get_master()
        if are_we_master:
            async with self.schema_lock:
                await self._subject_version_delete_local(content_type, subject, version, permanent)
        elif are_we_master is None:
            self.no_master_error(content_type)
        else:
            url = f"{master_url}/subjects/{subject}/versions/{version}?permanent={permanent}"
            await self.forward_request_remote(body={}, url=url, content_type=content_type, method="DELETE")

    async def subject_version_schema_get(self, content_type, *, subject, version):
        self._validate_version(content_type, version)
        subject_data = self._subject_get(subject, content_type)

        max_version = max(subject_data["schemas"])
        if version == "latest":
            schema_data = subject_data["schemas"][max_version]
        elif int(version) <= max_version:
            schema_data = subject_data["schemas"].get(int(version))
        else:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        self.r(schema_data["schema"].schema_str, content_type)

    async def subject_versions_list(self, content_type, *, subject):
        subject_data = self._subject_get(subject, content_type)
        self.r(list(subject_data["schemas"]), content_type, status=HTTPStatus.OK)

    async def get_master(self):
        async with self.master_lock:
            while True:
                master, master_url = self.mc.get_master_info()
                if master is None:
                    self.log.info("No master set: %r, url: %r", master, master_url)
                elif self.ksr.ready is False:
                    self.log.info("Schema reader isn't ready yet: %r", self.ksr.ready)
                else:
                    return master, master_url
                await asyncio.sleep(1.0)

    def _validate_schema_request_body(self, content_type, body) -> None:
        if not isinstance(body, dict):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                    "message": "Internal Server Error",
                },
                content_type=content_type,
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
        for field in body:
            if field not in {"schema", "schemaType"}:
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.HTTP_UNPROCESSABLE_ENTITY.value,
                        "message": f"Unrecognized field: {field}",
                    },
                    content_type=content_type,
                    status=HTTPStatus.UNPROCESSABLE_ENTITY,
                )

    def _validate_schema_type(self, content_type, body) -> None:
        schema_type = SchemaType(body.get("schemaType", SchemaType.AVRO.value))
        if schema_type not in {SchemaType.JSONSCHEMA, SchemaType.AVRO}:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_UNPROCESSABLE_ENTITY.value,
                    "message": f"unrecognized schemaType: {schema_type}",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

    def _validate_schema_key(self, content_type, body) -> None:
        if "schema" not in body:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                    "message": "Internal Server Error",
                },
                content_type=content_type,
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    async def subjects_schema_post(self, content_type, *, subject, request):
        body = request.json
        self._validate_schema_request_body(content_type, body)
        subject_data = self._subject_get(subject, content_type)
        new_schema = None
        if "schema" not in body:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                    "message": "Internal Server Error",
                },
                content_type=content_type,
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
        schema_str = body["schema"]
        schema_type = SchemaType(body.get("schemaType", "AVRO"))
        try:
            new_schema = TypedSchema.parse(schema_type, schema_str)
        except InvalidSchema:
            self.log.exception("No proper parser found")
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                    "message": f"Error while looking up schema under subject {subject}",
                },
                content_type=content_type,
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
        for schema in subject_data["schemas"].values():
            typed_schema = schema["schema"]
            if typed_schema == new_schema:
                ret = {
                    "subject": subject,
                    "version": schema["version"],
                    "id": schema["id"],
                    "schema": typed_schema.schema_str,
                }
                if schema_type is not SchemaType.AVRO:
                    ret["schemaType"] = schema_type
                self.r(ret, content_type)
            else:
                self.log.debug("Schema %r did not match %r", schema, typed_schema)
        self.r(
            body={
                "error_code": SchemaErrorCodes.SCHEMA_NOT_FOUND.value,
                "message": "Schema not found",
            },
            content_type=content_type,
            status=HTTPStatus.NOT_FOUND,
        )

    async def subject_post(self, content_type, *, subject, request):
        body = request.json
        self.log.debug("POST with subject: %r, request: %r", subject, body)
        self._validate_schema_request_body(content_type, body)
        self._validate_schema_type(content_type, body)
        self._validate_schema_key(content_type, body)
        are_we_master, master_url = await self.get_master()
        if are_we_master:
            async with self.schema_lock:
                await self.write_new_schema_local(subject, body, content_type)
        elif are_we_master is None:
            self.no_master_error(content_type)
        else:
            url = f"{master_url}/subjects/{subject}/versions"
            await self.forward_request_remote(body=body, url=url, content_type=content_type, method="POST")

    def write_new_schema_local(self, subject, body, content_type):
        """Since we're the master we get to write the new schema"""
        self.log.info("Writing new schema locally since we're the master")
        schema_type = SchemaType(body.get("schemaType", SchemaType.AVRO))
        try:
            new_schema = TypedSchema.parse(schema_type=schema_type, schema_str=body["schema"])
        except (InvalidSchema, InvalidSchemaType):
            self.log.warning("Invalid schema: %r", body["schema"], exc_info=True)
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_AVRO_SCHEMA.value,
                    "message": f"Invalid {schema_type} schema",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )
        if subject not in self.ksr.subjects or not self.ksr.subjects.get(subject)["schemas"]:
            schema_id = self.ksr.get_schema_id(new_schema)
            version = 1
            self.log.info(
                "Registering new subject: %r with version: %r to schema %r, schema_id: %r", subject, version,
                new_schema.schema_str, schema_id
            )
        else:
            # First check if any of the existing schemas for the subject match
            subject_data = self.ksr.subjects[subject]
            schemas = self.ksr.get_schemas(subject)
            if not schemas:  # Previous ones have been deleted by the user.
                version = max(self.ksr.subjects[subject]["schemas"]) + 1
                schema_id = self.ksr.get_schema_id(new_schema)
                self.log.info(
                    "Registering subject: %r, id: %r new version: %r with schema %r, schema_id: %r", subject, schema_id,
                    version, new_schema.schema_str, schema_id
                )
                self.send_schema_message(
                    subject=subject,
                    schema=new_schema,
                    schema_id=schema_id,
                    version=version,
                    deleted=False,
                )
                self.r({"id": schema_id}, content_type)

            schema_versions = sorted(list(schemas))
            # Go through these in version order
            for version in schema_versions:
                schema = subject_data["schemas"][version]
                if schema["schema"] == new_schema:
                    self.r({"id": schema["id"]}, content_type)
                else:
                    self.log.debug("schema: %s did not match with: %s", schema, new_schema)

            compatibility_mode = self._get_compatibility_mode(subject=subject_data, content_type=content_type)

            # Run a compatibility check between on file schema(s) and the one being submitted now
            # the check is either towards the latest one or against all previous ones in case of
            # transitive mode
            if compatibility_mode.is_transitive():
                check_against = schema_versions
            else:
                check_against = [schema_versions[-1]]

            for old_version in check_against:
                old_schema = subject_data["schemas"][old_version]["schema"]
                result = check_compatibility(
                    old_schema=old_schema,
                    new_schema=new_schema,
                    compatibility_mode=compatibility_mode,
                )
                if is_incompatible(result):
                    message = set(result.messages).pop() if result.messages else ""
                    self.log.warning("Incompatible schema: %s", result)
                    self.r(
                        body={
                            "error_code": SchemaErrorCodes.HTTP_CONFLICT.value,
                            "message": f"Incompatible schema, compatibility_mode={compatibility_mode.value} {message}",
                        },
                        content_type=content_type,
                        status=HTTPStatus.CONFLICT,
                    )

            # We didn't find an existing schema and the schema is compatible so go and create one
            schema_id = self.ksr.get_schema_id(new_schema)
            version = max(self.ksr.subjects[subject]["schemas"]) + 1
            self.log.info(
                "Registering subject: %r, id: %r new version: %r with schema %r, schema_id: %r", subject, schema_id, version,
                new_schema.to_json(), schema_id
            )
        self.send_schema_message(
            subject=subject,
            schema=new_schema,
            schema_id=schema_id,
            version=version,
            deleted=False,
        )
        self.r({"id": schema_id}, content_type)

    async def forward_request_remote(self, *, body, url, content_type, method="POST"):
        self.log.info("Writing new schema to remote url: %r since we're not the master", url)
        response = await self.http_request(url=url, method=method, json=body, timeout=60.0)
        self.r(body=response.body, content_type=content_type, status=HTTPStatus(response.status))

    def no_master_error(self, content_type):
        self.r(
            body={
                "error_code": SchemaErrorCodes.NO_MASTER_ERROR.value,
                "message": "Error while forwarding the request to the master.",
            },
            content_type=content_type,
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )


def main() -> int:
    parser = argparse.ArgumentParser(prog="karapace", description="Karapace: Your Kafka essentials in one tool")
    parser.add_argument("--version", action="version", help="show program version", version=karapace_version.__version__)
    parser.add_argument("config_file", help="configuration file path", type=argparse.FileType())
    arg = parser.parse_args()

    with closing(arg.config_file):
        config = read_config(arg.config_file)

    logging.getLogger().setLevel(config["log_level"])
    kc = KarapaceSchemaRegistry(config_file_path=arg.config_file.name, config=config)
    try:
        kc.run(host=kc.config["host"], port=kc.config["port"])
    except Exception:  # pylint: disable-broad-except
        if kc.raven_client:
            kc.raven_client.captureException(tags={"where": "karapace"})
        raise

    return 0


if __name__ == "__main__":
    sys.exit(main())
