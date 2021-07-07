from contextlib import closing
from karapace import version as karapace_version
from karapace.config import DEFAULT_LOG_FORMAT_JOURNAL, read_config
from karapace.kafka_rest_apis import KafkaRest
from karapace.rapu import RestApp
from karapace.schema_registry_apis import KarapaceSchemaRegistry

import argparse
import logging
import sys


class KarapaceAll(KafkaRest, KarapaceSchemaRegistry):
    # pylint: disable=super-init-not-called
    def __init__(self, config: dict) -> None:
        super().__init__(config=config)
        self.log = logging.getLogger("KarapaceAll")
        self.app.on_shutdown.append(self.close_by_app)

    async def close_by_app(self, app):
        # pylint: disable=unused-argument
        await self.close()


def main() -> int:
    parser = argparse.ArgumentParser(prog="karapace", description="Karapace: Your Kafka essentials in one tool")
    parser.add_argument("--version", action="version", help="show program version", version=karapace_version.__version__)
    parser.add_argument("config_file", help="configuration file path", type=argparse.FileType())
    arg = parser.parse_args()

    with closing(arg.config_file):
        config = read_config(arg.config_file)

    logging.basicConfig(level=logging.INFO, format=DEFAULT_LOG_FORMAT_JOURNAL)
    logging.getLogger().setLevel(config["log_level"])

    kc: RestApp
    if config["karapace_rest"] and config["karapace_registry"]:
        info_str = "both services"
        kc = KarapaceAll(config=config)
    elif config["karapace_rest"]:
        info_str = "karapace rest"
        kc = KafkaRest(config=config)
    elif config["karapace_registry"]:
        info_str = "karapace schema registry"
        kc = KarapaceSchemaRegistry(config=config)
    else:
        print("Both rest and registry options are disabled, exiting")
        return 1

    print("=" * 100 + f"\nStarting {info_str}\n" + "=" * 100)

    try:
        kc.run(host=kc.config["host"], port=kc.config["port"])
    except Exception:  # pylint: disable-broad-except
        if kc.raven_client:
            kc.raven_client.captureException(tags={"where": "karapace"})
        raise
    return 0


if __name__ == "__main__":
    sys.exit(main())
