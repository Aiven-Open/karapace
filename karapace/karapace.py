"""
karapace - main

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""

from functools import partial
from kafka import KafkaProducer
from karapace.rapu import HTTPResponse, RestApp
from karapace.utils import KarapaceKafkaClient

import asyncio
import logging
import os
import time

LOG_FORMAT_JOURNAL = "%(name)-20s\t%(threadName)s\t%(levelname)-8s\t%(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT_JOURNAL)


class KarapaceBase(RestApp):
    def __init__(self, config_file_path: str, config: dict) -> None:
        self.config = {}
        self.producer = None
        self.kafka_timeout = 10
        self.config_path = config_file_path
        self.config = config
        self._sentry_config = self.config.get("sentry", {"dsn": None}).copy()
        if os.environ.get("SENTRY_DSN"):
            self._sentry_config["dsn"] = os.environ["SENTRY_DSN"]
        if "tags" not in self._sentry_config:
            self._sentry_config["tags"] = {}
        self._sentry_config["tags"]["app"] = "Karapace"

        super().__init__(app_name="karapace", sentry_config=self._sentry_config)
        self.route("/", callback=self.root_get, method="GET")
        self.log = logging.getLogger("Karapace")
        self.app.on_startup.append(self.create_http_client)
        self.master_lock = asyncio.Lock()
        self._set_log_level()
        self.log.info("Karapace initialized")

    def _create_producer(self):
        while True:
            try:
                return KafkaProducer(
                    bootstrap_servers=self.config["bootstrap_uri"],
                    security_protocol=self.config["security_protocol"],
                    ssl_cafile=self.config["ssl_cafile"],
                    ssl_certfile=self.config["ssl_certfile"],
                    ssl_keyfile=self.config["ssl_keyfile"],
                    api_version=(1, 0, 0),
                    metadata_max_age_ms=self.config["metadata_max_age_ms"],
                    max_block_ms=2000,  # missing topics will block unless we cache cluster metadata and pre-check
                    connections_max_idle_ms=self.config["connections_max_idle_ms"],  # helps through cluster upgrades ??
                    client_factory=KarapaceKafkaClient,
                )
            except:  # pylint: disable=bare-except
                self.log.exception("Unable to create producer, retrying")
                time.sleep(1)

    def close(self):
        if not self.producer:
            return
        self.producer.close()
        self.producer = None

    def _set_log_level(self):
        try:
            logging.getLogger().setLevel(self.config["log_level"])
        except ValueError:
            self.log.exception("Problem with log_level: %r", self.config["log_level"])

    @staticmethod
    def r(body, content_type, status=200):
        raise HTTPResponse(
            body=body,
            status=status,
            content_type=content_type,
            headers={},
        )

    @staticmethod
    def internal_error(message, content_type):
        KarapaceBase.r(content_type=content_type, status=500, body={"message": message, "error_code": 500})

    @staticmethod
    def unprocessable_entity(message, sub_code, content_type):
        KarapaceBase.r(content_type=content_type, status=422, body={"message": message, "error_code": sub_code})

    @staticmethod
    def topic_entity(message, sub_code, content_type):
        KarapaceBase.r(content_type=content_type, status=422, body={"message": message, "error_code": sub_code})

    @staticmethod
    def not_found(message, sub_code, content_type):
        KarapaceBase.r(content_type=content_type, status=404, body={"message": message, "error_code": sub_code})

    async def root_get(self):
        self.r({}, "application/json")


empty_response = partial(KarapaceBase.r, body={}, status=204, content_type="application/json")
