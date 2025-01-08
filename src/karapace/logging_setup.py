"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from karapace.config import Config

import logging
import sys


def configure_logging(*, config: Config) -> None:
    root_handler: logging.Handler | None = None

    log_handler = config.log_handler
    match log_handler:
        case "stdout" | None:
            root_handler = logging.StreamHandler(stream=sys.stdout)
        case "systemd":
            from systemd import journal

            root_handler = journal.JournalHandler(SYSLOG_IDENTIFIER="karapace")
        case _:
            logging.basicConfig(level=config.log_level.upper(), format=config.log_format)
            logging.getLogger().setLevel(config.log_level.upper())
            logging.warning("Log handler %s not recognized, root handler not set.", log_handler)

    if root_handler is not None:
        root_handler.setFormatter(logging.Formatter(config.log_format))
        root_handler.setLevel(config.log_level.upper())
        root_handler.set_name(name="karapace")
        logging.root.addHandler(root_handler)

    logging.root.setLevel(config.log_level.upper())


def log_config_without_secrets(config: Config) -> None:
    config_without_secrets = {}
    for key, value in config.dict().items():
        if "password" in key:
            value = "****"
        elif "keyfile" in key:
            value = "****"
        config_without_secrets[key] = value
    logging.log(logging.DEBUG, "Config %r", config_without_secrets)
