"""
karapace - main

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""

from karapace.config import set_config_defaults
from karapace.rapu import HTTPResponse, RestApp

import asyncio
import json
import logging
import os

LOG_FORMAT_JOURNAL = "%(name)-20s\t%(threadName)s\t%(levelname)-8s\t%(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT_JOURNAL)


class InvalidConfiguration(Exception):
    pass


class KarapaceBase(RestApp):
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
        self.route("/", callback=self.root_get, method="GET")
        self.log = logging.getLogger("Karapace")
        self.app.on_startup.append(self.create_http_client)
        self.master_lock = asyncio.Lock()
        self._set_log_level()
        self.log.info("Karapace initialized")

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
