from aiohttp.web_log import AccessLogger
from contextlib import closing
from karapace import version as karapace_version
from karapace.config import Config, read_config
from karapace.kafka_rest_apis import KafkaRest
from karapace.rapu import RestApp
from karapace.schema_registry_apis import KarapaceSchemaRegistry
from karapace.utils import DebugAccessLogger

import argparse
import logging
import sys


class KarapaceAll(KafkaRest, KarapaceSchemaRegistry):
    def __init__(self, config: Config) -> None:
        super().__init__(config=config)
        self.log = logging.getLogger("KarapaceAll")


def main() -> int:
    parser = argparse.ArgumentParser(prog="karapace", description="Karapace: Your Kafka essentials in one tool")
    parser.add_argument("--version", action="version", help="show program version", version=karapace_version.__version__)
    parser.add_argument("config_file", help="configuration file path", type=argparse.FileType())
    arg = parser.parse_args()

    with closing(arg.config_file):
        config = read_config(arg.config_file)

    logging.basicConfig(level=logging.INFO, format=config["log_format"])
    logging.getLogger().setLevel(config["log_level"])
    if config.get("access_logs_debug") is True:
        config["access_log_class"] = DebugAccessLogger
        logging.getLogger("aiohttp.access").setLevel(logging.DEBUG)
    else:
        config["access_log_class"] = AccessLogger

    app: RestApp
    if config["karapace_rest"] and config["karapace_registry"]:
        info_str = "both services"
        app = KarapaceAll(config=config)
    elif config["karapace_rest"]:
        info_str = "karapace rest"
        app = KafkaRest(config=config)
    elif config["karapace_registry"]:
        info_str = "karapace schema registry"
        app = KarapaceSchemaRegistry(config=config)
    else:
        print("Both rest and registry options are disabled, exiting")
        return 1

    info_str_separator = "=" * 100
    logging.log(logging.INFO, "\n%s\nStarting %s\n%s", info_str_separator, info_str, info_str_separator)

    try:
        app.run()
    except Exception:  # pylint: disable-broad-except
        if app.raven_client:
            app.raven_client.captureException(tags={"where": "karapace"})
        raise
    return 0


if __name__ == "__main__":
    sys.exit(main())
