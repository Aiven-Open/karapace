"""
karapace - main

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from kafka import KafkaProducer
from karapace import version as karapace_version
from karapace.compatibility import Compatibility, IncompatibleSchema
from karapace.config import set_config_defaults
from karapace.master_coordinator import MasterCoordinator
from karapace.rapu import HTTPResponse, RestApp
from karapace.schema_reader import KafkaSchemaReader
from karapace.utils import json_encode

import argparse
import asyncio
import avro.schema
import json
import logging
import os
import sys
import time

LOG_FORMAT_JOURNAL = "%(name)-20s\t%(threadName)s\t%(levelname)-8s\t%(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT_JOURNAL)

COMPATIBILITY_MODES = {
    "BACKWARD",
    "BACKWARD_TRANSITIVE",
    "FORWARD",
    "FORWARD_TRANSITIVE",
    "FULL",
    "FULL_TRANSITIVE",
    "NONE",
}

TRANSITIVE_MODES = {
    "BACKWARD_TRANSITIVE",
    "FORWARD_TRANSITIVE",
    "FULL_TRANSITIVE",
}


class InvalidConfiguration(Exception):
    pass


class Karapace(RestApp):
    def __init__(self, config_path):
        self.config = {}
        self.config_path = config_path
        self.config = self.read_config(self.config_path)
        self._sentry_config = self.config.get("sentry", {"dsn": None}).copy()
        if os.environ.get("SENTRY_DSN"):
            self._sentry_config["dsn"] = os.environ["SENTRY_DSN"]
        if "tags" not in self._sentry_config:
            self._sentry_config["tags"] = {}
        self._sentry_config["tags"]["app"] = "Karapace"

        super().__init__(app_name="Karapace", sentry_config=self._sentry_config)

        self.log = logging.getLogger("Karapace")
        self.kafka_timeout = 10

        self.route(
            "/compatibility/subjects/<subject:path>/versions/<version:path>",
            callback=self.compatibility_check,
            method="POST",
            schema_request=True
        )

        self.route("/", callback=self.root_get, method="GET")

        self.route("/config/<subject:path>", callback=self.config_subject_get, method="GET", schema_request=True)
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
            "/subjects/<subject:path>/versions/<version:path>",
            callback=self.subject_version_delete,
            method="DELETE",
            schema_request=True
        )
        self.route(
            "/subjects/<subject:path>/versions/<version>/schema",
            callback=self.subject_version_schema_get,
            method="GET",
            schema_request=True
        )
        self.route("/subjects/<subject:path>", callback=self.subject_delete, method="DELETE", schema_request=True)

        self.ksr = None
        self._set_log_level()
        self._create_producer()
        self._create_schema_reader()
        self._create_master_coordinator()
        self.app.on_startup.append(self.create_http_client)

        self.master_lock = asyncio.Lock()

        self.log.info("Karapace initialized")

    def close(self):
        self.log.info("Shutting down all auxiliary threads")
        if self.mc:
            self.mc.close()
        if self.ksr:
            self.ksr.close()
        if self.producer:
            self.producer.close()

    @staticmethod
    def read_config(config_path):
        if os.path.exists(config_path):
            try:
                config = json.loads(open(config_path, "r").read())
                config = set_config_defaults(config)
                return config
            except Exception as ex:
                raise InvalidConfiguration(ex)
        else:
            raise InvalidConfiguration()

    def _set_log_level(self):
        try:
            logging.getLogger().setLevel(self.config["log_level"])
        except ValueError:
            self.log.excption("Problem with log_level: %r", self.config["log_level"])

    def _create_schema_reader(self):
        self.ksr = KafkaSchemaReader(config=self.config, )
        self.ksr.start()

    def _create_master_coordinator(self):
        self.mc = MasterCoordinator(config=self.config)
        self.mc.start()

    def _create_producer(self):
        self.producer = KafkaProducer(
            bootstrap_servers=self.config["bootstrap_uri"],
            security_protocol=self.config["security_protocol"],
            ssl_cafile=self.config["ssl_cafile"],
            ssl_certfile=self.config["ssl_certfile"],
            ssl_keyfile=self.config["ssl_keyfile"],
            api_version=(1, 0, 0),
            metadata_max_age_ms=self.config["metadata_max_age_ms"],
        )

    def _subject_get(self, subject, content_type):
        subject_data = self.ksr.subjects.get(subject)
        if not subject_data:
            self.r({"error_code": 40401, "message": "Subject not found."}, content_type, status=404)

        schemas = self.ksr.get_schemas(subject)
        if not schemas:
            self.r({"error_code": 40401, "message": "Subject not found."}, content_type, status=404)

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
                "error_code": 42202,
                "message": "The specified version is not a valid version id. "
                "Allowed values are between [1, 2^31-1] and the string \"latest\""
            },
            content_type=content_type,
            status=422
        )

    @staticmethod
    def r(body, content_type, status=200):
        raise HTTPResponse(
            body=body,
            status=status,
            content_type=content_type,
            headers={},
        )

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
            self.log.error("Put the offset: %r back to queue, someone else is waiting for this?", offset)
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

    def send_schema_message(self, subject, parsed_schema_json, schema_id, version, deleted):
        key = '{{"subject":"{}","version":{},"magic":1,"keytype":"SCHEMA"}}'.format(subject, version)
        value = {
            "subject": subject,
            "version": version,
            "id": schema_id,
            "schema": json_encode(parsed_schema_json, compact=True),
            "deleted": deleted
        }
        return self.send_kafka_message(key, json_encode(value, compact=True))

    def send_config_message(self, compatibility_level, subject=None):
        if subject is not None:
            key = '{{"subject":"{}","magic":0,"keytype":"CONFIG"}}'.format(subject)
        else:
            key = '{"subject":null,"magic":0,"keytype":"CONFIG"}'
        value = '{{"compatibilityLevel":"{}"}}'.format(compatibility_level)
        return self.send_kafka_message(key, value)

    def send_delete_subject_message(self, subject, version):
        key = '{{"subject":"{}","magic":0,"keytype":"DELETE_SUBJECT"}}'.format(subject)
        value = '{{"subject":"{}","version":{}}}'.format(subject, version)
        return self.send_kafka_message(key, value)

    async def root_get(self):
        self.r({}, "application/json")

    async def compatibility_check(self, content_type, *, subject, version, request):
        """Check for schema compatibility"""
        body = request.json
        self.log.info("Got request to check subject: %r, version_id: %r compatibility", subject, version)
        old = await self.subject_version_get(content_type=content_type, subject=subject, version=version, return_dict=True)
        self.log.info("Existing schema: %r, new_schema: %r", old["schema"], body["schema"])
        try:
            new = avro.schema.Parse(body["schema"])
        except avro.schema.SchemaParseException:
            self.log.warning("Invalid schema: %r", body["schema"])
            self.r(body={"error_code": 44201, "message": "Invalid Avro schema"}, content_type=content_type, status=422)
        try:
            old_schema = avro.schema.Parse(old["schema"])
        except avro.schema.SchemaParseException:
            self.log.warning("Invalid existing schema: %r", old["schema"])
            self.r(body={"error_code": 44201, "message": "Invalid Avro schema"}, content_type=content_type, status=422)

        compat = Compatibility(
            source=old_schema, target=new, compatibility=old.get("compatibility", self.ksr.config["compatibility"])
        )
        try:
            compat.check()
        except IncompatibleSchema as ex:
            self.log.warning("Invalid schema %s found by compatibility check: old: %s new: %s", ex, old_schema, new)
            self.r({"is_compatible": False}, content_type)
        self.r({"is_compatible": True}, content_type)

    async def schemas_get(self, content_type, *, schema_id):
        try:
            schema_id_int = int(schema_id)
        except ValueError:
            self.r({"error_code": 404, "message": "HTTP 404 Not Found"}, content_type, status=404)
        schema = self.ksr.schemas.get(schema_id_int)
        if not schema:
            self.log.warning("Schema: %r that was requested, not found", int(schema_id))
            self.r(body={"error_code": 40403, "message": "Schema not found"}, content_type=content_type, status=404)
        self.r({"schema": schema}, content_type)

    async def config_get(self, content_type):
        self.r({"compatibilityLevel": self.ksr.config["compatibility"]}, content_type)

    async def config_set(self, content_type, *, request):
        if "compatibility" in request.json and request.json["compatibility"] in COMPATIBILITY_MODES:
            compatibility_level = request.json["compatibility"]
            self.send_config_message(compatibility_level=compatibility_level, subject=None)
        else:
            self.r(
                body={
                    "error_code": 42203,
                    "message": "Invalid compatibility level. Valid values are none, backward, forward and full"
                },
                content_type=content_type,
                status=422
            )
        self.r({"compatibility": self.ksr.config["compatibility"]}, content_type)

    async def config_subject_get(self, content_type, *, subject):
        # Config for a subject can exist without schemas so no need to check for their existence
        subject_data = self.ksr.subjects.get(subject)

        if "compatibility" in subject_data:
            self.r({"compatibilityLevel": subject_data["compatibility"]}, content_type)

        self.r({"error_code": 40401, "message": "Subject not found."}, content_type, status=404)

    async def config_subject_set(self, content_type, *, request, subject):
        if "compatibility" in request.json and request.json["compatibility"] in COMPATIBILITY_MODES:
            self.send_config_message(compatibility_level=request.json["compatibility"], subject=subject)
        else:
            self.r(
                body={
                    "error_code": 42203,
                    "message": "Invalid compatibility level"
                }, content_type=content_type, status=422
            )

        self.r({"compatibility": request.json["compatibility"]}, content_type)

    async def subjects_list(self, content_type):
        subjects_list = [key for key, val in self.ksr.subjects.items() if self.ksr.get_schemas(key)]
        self.r(subjects_list, content_type, status=200)

    async def subject_delete(self, content_type, *, subject):
        self._subject_get(subject, content_type)
        version_list = list(self.ksr.get_schemas(subject))
        if version_list:
            latest_schema_id = version_list[-1]
        else:
            latest_schema_id = 0
        self.send_delete_subject_message(subject, latest_schema_id)
        self.r(version_list, content_type, status=200)

    async def subject_version_get(self, content_type, *, subject, version, return_dict=False):
        self._validate_version(content_type, version)
        subject_data = self._subject_get(subject, content_type)

        max_version = max(subject_data["schemas"])
        if version == "latest":
            schema = subject_data["schemas"][max(subject_data["schemas"])]
            version = max(subject_data["schemas"])
        elif int(version) <= max_version:
            schema = subject_data["schemas"].get(int(version))
        else:
            self.r({"error_code": 40402, "message": "Version not found."}, content_type, status=404)

        schema_string = schema["schema"]
        schema_id = schema["id"]
        ret = {
            "subject": subject,
            "version": int(version),
            "id": schema_id,
            "schema": schema_string,
        }
        if return_dict:
            # Return also compatibility information to compatibility check
            if subject_data.get("compatibility"):
                ret["compatibility"] = subject_data.get("compatibility")
            return ret
        self.r(ret, content_type)

    async def subject_version_delete(self, content_type, *, subject, version):
        version = int(version)
        subject_data = self._subject_get(subject, content_type)

        schema = subject_data["schemas"].get(version, None)
        if not schema:
            self.r({"error_code": 40402, "message": "Version not found."}, content_type, status=404)
        schema_id = schema["id"]
        self.send_schema_message(subject, schema, schema_id, version, deleted=True)
        self.r(str(version), content_type, status=200)

    async def subject_version_schema_get(self, content_type, *, subject, version):
        self._validate_version(content_type, version)
        subject_data = self._subject_get(subject, content_type)

        max_version = max(subject_data["schemas"])
        if version == "latest":
            schema_data = subject_data["schemas"][max_version]
        elif int(version) <= max_version:
            schema_data = subject_data["schemas"].get(int(version))
        else:
            self.r({"error_code": 40402, "message": "Version not found."}, content_type, status=404)
        self.r(schema_data["schema"], content_type)

    async def subject_versions_list(self, content_type, *, subject):
        subject_data = self._subject_get(subject, content_type)
        self.r(list(subject_data["schemas"]), content_type, status=200)

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

    def _validate_schema_request_body(self, content_type, body):
        if not isinstance(body, dict):
            self.r({"error_code": 500, "message": "Internal Server Error"}, content_type, status=500)
        for field in body:
            if field != "schema":
                self.r(
                    body={
                        "error_code": 422,
                        "message": f"Unrecognized field: {field}",
                    },
                    content_type=content_type,
                    status=422
                )

    async def subjects_schema_post(self, content_type, *, subject, request):
        body = request.json
        self._validate_schema_request_body(content_type, body)
        subject_data = self._subject_get(subject, content_type)
        if "schema" not in body:
            self.r({"error_code": 500, "message": "Internal Server Error"}, content_type, status=500)
        try:
            new_schema = avro.schema.Parse(body["schema"])
        except avro.schema.SchemaParseException:
            self.r(
                body={
                    "error_code": 500,
                    "message": f"Error while looking up schema under subject {subject}"
                },
                content_type=content_type,
                status=500
            )

        new_schema_encoded = json_encode(new_schema.to_json(), compact=True)
        for schema in subject_data["schemas"].values():
            if schema["schema"] == new_schema_encoded:
                ret = {
                    "subject": subject,
                    "version": schema["version"],
                    "id": schema["id"],
                    "schema": schema["schema"],
                }
                self.r(ret, content_type)
        self.r({"error_code": 40403, "message": "Schema not found"}, content_type, status=404)

    async def subject_post(self, content_type, *, subject, request):
        body = request.json
        self.log.debug("POST with subject: %r, request: %r", subject, body)
        self._validate_schema_request_body(content_type, body)
        if "schema" not in body:
            self.r({"error_code": 500, "message": "Internal Server Error"}, content_type, status=500)
        are_we_master, master_url = await self.get_master()
        if are_we_master:
            self.write_new_schema_local(subject, body, content_type)
        elif are_we_master is None:
            self.r({
                "error_code": 50003,
                "message": "Error while forwarding the request to the master."
            },
                   content_type,
                   status=500)
        else:
            await self.write_new_schema_remote(subject, body, master_url, content_type)

    def write_new_schema_local(self, subject, body, content_type):
        """Since we're the master we get to write the new schema"""
        self.log.info("Writing new schema locally since we're the master")
        try:
            new_schema = avro.schema.Parse(body["schema"])
        except avro.schema.SchemaParseException:
            self.log.warning("Invalid schema: %r", body["schema"])
            self.r(body={"error_code": 44201, "message": "Invalid Avro schema"}, content_type=content_type, status=422)

        if subject not in self.ksr.subjects or not self.ksr.subjects.get(subject)["schemas"]:
            schema_id = self.ksr.get_schema_id(new_schema)
            version = 1
            self.log.info(
                "Registering new subject: %r with version: %r to schema %r, schema_id: %r", subject, version,
                new_schema.to_json(), schema_id
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
                    version, new_schema.to_json(), schema_id
                )
                self.send_schema_message(subject, new_schema.to_json(), schema_id, version, deleted=False)
                self.r({"id": schema_id}, content_type)

            schema_versions = sorted(list(schemas))
            # Go through these in version order
            for version in schema_versions:
                schema = subject_data["schemas"][version]
                parsed_version_schema = avro.schema.Parse(schema["schema"])
                if parsed_version_schema == new_schema:
                    self.r({"id": schema["id"]}, content_type)
                else:
                    self.log.debug("schema: %s did not match with: %s", schema["schema"], new_schema.to_json())

            compatibility = subject_data.get("compatibility", self.ksr.config["compatibility"])

            # Run a compatibility check between on file schema(s) and the one being submitted now
            # the check is either towards the latest one or against all previous ones in case of
            # transitive mode
            if compatibility in TRANSITIVE_MODES:
                check_against = schema_versions
            else:
                check_against = [schema_versions[-1]]

            for old_version in check_against:
                old_schema = avro.schema.Parse(subject_data["schemas"][old_version]["schema"])
                compat = Compatibility(old_schema, new_schema, compatibility=compatibility)
                try:
                    compat.check()
                except IncompatibleSchema as ex:
                    self.log.warning("Incompatible schema: %s", ex)
                    self.r(
                        body={
                            "error_code": 409,
                            "message": "Schema being registered is incompatible with an earlier schema"
                        },
                        content_type=content_type,
                        status=409
                    )

            # We didn't find an existing schema and the schema is compatible so go and create one
            schema_id = self.ksr.get_schema_id(new_schema)
            version = max(self.ksr.subjects[subject]["schemas"]) + 1
            self.log.info(
                "Registering subject: %r, id: %r new version: %r with schema %r, schema_id: %r", subject, schema_id, version,
                new_schema.to_json(), schema_id
            )
        self.send_schema_message(subject, new_schema.to_json(), schema_id, version, deleted=False)
        self.r({"id": schema_id}, content_type)

    async def write_new_schema_remote(self, subject, body, master_url, content_type):
        self.log.info("Writing new schema to remote url: %r since we're not the master", master_url)
        response = await self.http_request(
            url="{}/subjects/{}/versions".format(master_url, subject), method="POST", json=body, timeout=60.0
        )
        self.r(body=response.body, content_type=content_type, status=response.status)


def main():
    parser = argparse.ArgumentParser(prog="karapace", description="Karapace: Your Kafka essentials in one tool")
    parser.add_argument("--version", action="version", help="show program version", version=karapace_version.__version__)
    parser.add_argument("config_file", help="configuration file path")
    arg = parser.parse_args()

    if not os.path.exists(arg.config_file):
        print("Config file: {} does not exist, exiting".format(arg.config_file))
        return 1

    kc = Karapace(arg.config_file)
    try:
        return kc.run(host=kc.config["host"], port=kc.config["port"])
    except Exception:  # pylint: disable-broad-except
        if kc.raven_client:
            kc.raven_client.captureException(tags={"where": "karapace"})
        raise


if __name__ == "__main__":
    sys.exit(main())
