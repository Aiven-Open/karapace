"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from aiohttp.web_log import AccessLogger
from contextlib import closing
from karapace import version as karapace_version
from karapace.config import Config, read_config
from karapace.instrumentation.prometheus import PrometheusInstrumentation
from karapace.utils import DebugAccessLogger
from rest_proxy import KafkaRest
from typing import Final

import argparse
import logging
import sys

PROGRAM_NAME: Final[str] = "karapace_rest_proxy"


def _configure_logging(*, config: Config) -> None:
    log_level = config.get("log_level", "DEBUG")
    log_format = config.get("log_format", "%(name)-20s\t%(threadName)s\t%(levelname)-8s\t%(message)s")

    root_handler: logging.Handler | None = None
    log_handler = config.get("log_handler", None)
    if "systemd" == log_handler:
        from systemd import journal

        root_handler = journal.JournalHandler(SYSLOG_IDENTIFIER="karapace")
    elif "stdout" == log_handler or log_handler is None:
        root_handler = logging.StreamHandler(stream=sys.stdout)
    else:
        logging.basicConfig(level=logging.INFO, format=log_format)
        logging.getLogger().setLevel(log_level)
        logging.warning("Log handler %s not recognized, root handler not set.", log_handler)

    if root_handler is not None:
        root_handler.setFormatter(logging.Formatter(log_format))
        root_handler.setLevel(log_level)
        root_handler.set_name(name="karapace")
        logging.root.addHandler(root_handler)

    logging.root.setLevel(log_level)

    if config.get("access_logs_debug") is True:
        config["access_log_class"] = DebugAccessLogger
        logging.getLogger("aiohttp.access").setLevel(logging.DEBUG)
    else:
        config["access_log_class"] = AccessLogger


def main() -> int:
    parser = argparse.ArgumentParser(
        prog=PROGRAM_NAME,
        description="Karapace rest proxy: exposes an API over common Kafka operations, your Kafka essentials in one tool",
    )
    parser.add_argument("--version", action="version", help="show program version", version=karapace_version.__version__)
    parser.add_argument("config_file", help="configuration file path", type=argparse.FileType())
    arg = parser.parse_args()

    with closing(arg.config_file):
        config = read_config(arg.config_file)

    logging.log(logging.INFO, "\n%s\\Co %s\n%s", ("=" * 50), PROGRAM_NAME, ("=" * 50))

    _configure_logging(config=config)

    app = KafkaRest(config=config)

    logging.log(logging.INFO, "\n%s\nStarting %s\n%s", ("=" * 50), PROGRAM_NAME, ("=" * 50))

    config_without_secrets = {}
    for key, value in config.items():
        if "password" in key:
            value = "****"
        config_without_secrets[key] = value
    logging.log(logging.DEBUG, "Config %r", config_without_secrets)

    try:
        PrometheusInstrumentation.setup_metrics(app=app)
        app.run()  # `close` will be called by the callback `close_by_app` set by `KarapaceBase`
    except Exception as ex:  # pylint: disable-broad-except
        app.stats.unexpected_exception(ex=ex, where=f"{PROGRAM_NAME}_main")
        raise

    return 0


if __name__ == "__main__":
    sys.exit(main())
