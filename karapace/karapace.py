"""
karapace - main

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""

from kafka import KafkaProducer
from karapace.config import read_config
from karapace.rapu import HTTPResponse, RestApp

import asyncio
import logging
import os

LOG_FORMAT_JOURNAL = "%(name)-20s\t%(threadName)s\t%(levelname)-8s\t%(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT_JOURNAL)


class KarapaceBase(RestApp):
    def __init__(self, config_path):
        self.config = {}
        self.producer = None
        self.config_path = config_path
        self.config = self.read_config(self.config_path)
        self._sentry_config = self.config.get("sentry", {"dsn": None}).copy()
        if os.environ.get("SENTRY_DSN"):
            self._sentry_config["dsn"] = os.environ["SENTRY_DSN"]
        if "tags" not in self._sentry_config:
            self._sentry_config["tags"] = {}
        self._sentry_config["tags"]["app"] = "Karapace"

        super().__init__(app_name="Karapace", sentry_config=self._sentry_config)
        self.route("/", callback=self.root_get, method="GET")
        self.log = logging.getLogger("Karapace")
        self.app.on_startup.append(self.create_http_client)
        self.master_lock = asyncio.Lock()
        self._set_log_level()
        self.log.info("Karapace initialized")

    def _create_producer(self):
        self.producer = KafkaProducer(
            bootstrap_servers=self.config["bootstrap_uri"],
            security_protocol=self.config["security_protocol"],
            ssl_cafile=self.config["ssl_cafile"],
            ssl_certfile=self.config["ssl_certfile"],
            ssl_keyfile=self.config["ssl_keyfile"],
            api_version=(1, 0, 0),
            metadata_max_age_ms=self.config["metadata_max_age_ms"],
            max_block_ms=2000  # missing topics will block unless we cache cluster metadata and pre-check
        )

    def close(self):
        if not self.producer:
            return
        self.producer.close()
        self.producer = None

    @staticmethod
    def read_config(config_path):
        return read_config(config_path)

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

    async def root_get(self):
        self.r({}, "application/json")
