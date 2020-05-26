from karapace import version as karapace_version
from karapace.config import InvalidConfiguration, read_config
from karapace.kafka_rest_apis import KafkaRest
from karapace.karapace import KarapaceBase
from karapace.schema_registry_apis import KarapaceSchemaRegistry

import argparse
import logging
import os
import sys


class KarapaceAll(KafkaRest, KarapaceSchemaRegistry, KarapaceBase):
    # pylint: disable=super-init-not-called
    def __init__(self, config):
        KarapaceBase.__init__(self, config)
        KafkaRest._init(self, config)
        KafkaRest._add_routes(self)
        KarapaceSchemaRegistry._init(self)
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


def main():
    parser = argparse.ArgumentParser(prog="karapace", description="Karapace: Your Kafka essentials in one tool")
    parser.add_argument("--version", action="version", help="show program version", version=karapace_version.__version__)
    parser.add_argument("config_file", help="configuration file path")
    arg = parser.parse_args()

    if not os.path.exists(arg.config_file):
        print("Config file: {} does not exist, exiting".format(arg.config_file))
        return 1
    config = None
    kc = None
    info_str = ""
    try:
        config = read_config(arg.config_file)
    except InvalidConfiguration:
        print("Config file: {} is invalid, exiting".format(arg.config_file))
        return 1
    if config["karapace_rest"] and config["karapace_registry"]:
        info_str = "both services"
        kc = KarapaceAll(arg.config_file)
    elif config["karapace_rest"]:
        info_str = "karapace rest"
        kc = KafkaRest(arg.config_file)
    elif config["karapace_registry"]:
        info_str = "karapace schema registry"
        kc = KarapaceSchemaRegistry(arg.config_file)
    else:
        print("Both rest and registry options are disabled, exiting")
        return 1
    print("=" * 100 + f"\nStarting {info_str}\n" + "=" * 100)
    try:
        return kc.run(host=kc.config["host"], port=kc.config["port"])
    except Exception:  # pylint: disable-broad-except
        if kc.raven_client:
            kc.raven_client.captureException(tags={"where": "karapace"})
        raise


if __name__ == "__main__":
    sys.exit(main())
