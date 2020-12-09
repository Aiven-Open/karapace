from contextlib import closing
from karapace import version as karapace_version
from karapace.config import read_config
from karapace.kafka_rest_apis import KafkaRest
from karapace.karapace import KarapaceBase
from karapace.rapu import RestApp
from karapace.schema_registry_apis import KarapaceSchemaRegistry

import argparse
import logging
import sys


class KarapaceAll(KafkaRest, KarapaceSchemaRegistry, KarapaceBase):
    # pylint: disable=super-init-not-called
    def __init__(self, config_file_path: str, config: dict) -> None:
        KarapaceBase.__init__(self, config_file_path=config_file_path, config=config)
        KafkaRest._init(self, config=config)
        KafkaRest._add_routes(self)
        KarapaceSchemaRegistry._init(self, config=config)
        KarapaceSchemaRegistry._add_routes(self)
        self.log = logging.getLogger("KarapaceAll")
        self.app.on_shutdown.append(self.close_by_app)

    def close_by_app(self, app):
        # pylint: disable=unused-argument
        self.close()

    def close(self):
        for cls in [KarapaceBase, KafkaRest, KarapaceSchemaRegistry]:
            try:
                cls.close(self)
            except:  # pylint: disable=bare-except
                pass
            KafkaRest.close_producers(self)


def main() -> int:
    parser = argparse.ArgumentParser(prog="karapace", description="Karapace: Your Kafka essentials in one tool")
    parser.add_argument("--version", action="version", help="show program version", version=karapace_version.__version__)
    parser.add_argument("config_file", help="configuration file path", type=argparse.FileType())
    arg = parser.parse_args()

    with closing(arg.config_file):
        config = read_config(arg.config_file)

    config_file_path = arg.config_file.name

    kc: RestApp
    if config["karapace_rest"] and config["karapace_registry"]:
        info_str = "both services"
        kc = KarapaceAll(config_file_path=config_file_path, config=config)
    elif config["karapace_rest"]:
        info_str = "karapace rest"
        kc = KafkaRest(config_file_path=config_file_path, config=config)
    elif config["karapace_registry"]:
        info_str = "karapace schema registry"
        kc = KarapaceSchemaRegistry(config_file_path=config_file_path, config=config)
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
