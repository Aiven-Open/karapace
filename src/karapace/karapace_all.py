"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from contextlib import closing
from karapace import version as karapace_version
from karapace.config import read_config
from karapace.instrumentation.prometheus import PrometheusInstrumentation
from karapace.kafka_rest_apis import KafkaRest
from karapace.logging import configure_logging
from karapace.rapu import RestApp
from schema_registry.schema_registry_apis import KarapaceSchemaRegistryController

import argparse
import logging
import sys


class KarapaceAll(KafkaRest, KarapaceSchemaRegistryController):
    pass


def main() -> int:
    parser = argparse.ArgumentParser(prog="karapace", description="Karapace: Your Kafka essentials in one tool")
    parser.add_argument("--version", action="version", help="show program version", version=karapace_version.__version__)
    parser.add_argument("config_file", help="configuration file path", type=argparse.FileType())
    arg = parser.parse_args()

    with closing(arg.config_file):
        config = read_config(arg.config_file)

    configure_logging(config=config)

    app: RestApp
    if config["karapace_rest"] and config["karapace_registry"]:
        info_str = "both services"
        app = KarapaceAll(config=config)
    elif config["karapace_rest"]:
        info_str = "karapace rest"
        app = KafkaRest(config=config)
    elif config["karapace_registry"]:
        info_str = "karapace schema registry"
        app = KarapaceSchemaRegistryController(config=config)
    else:
        print("Both rest and registry options are disabled, exiting")
        return 1

    info_str_separator = "=" * 100
    logging.log(logging.INFO, "\n%s\nStarting %s\n%s", info_str_separator, info_str, info_str_separator)

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
        app.stats.unexpected_exception(ex=ex, where="karapace")
        raise
    return 0


if __name__ == "__main__":
    sys.exit(main())
