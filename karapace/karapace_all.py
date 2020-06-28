from karapace import version as karapace_version
from karapace.config import InvalidConfiguration, read_config
from karapace.kafka_rest_apis import KafkaRest
from karapace.karapace import KarapaceBase
from karapace.schema_registry_apis import KarapaceSchemaRegistry
from typing import Optional

import argparse
import asyncio
import enum
import logging
import os
import signal
import sys


@enum.unique
class _AppType(str, enum.Enum):
    none = "none"
    registry = "registry"
    rest = "rest"
    both = "both"


class KarapaceAll(KafkaRest, KarapaceSchemaRegistry, KarapaceBase):
    # pylint: disable=super-init-not-called
    def __init__(self, config, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.loop = loop or asyncio.get_event_loop
        KarapaceBase.__init__(self, config)
        KafkaRest._init(self, config)
        KafkaRest._add_routes(self)
        KarapaceSchemaRegistry._init(self)
        KarapaceSchemaRegistry._add_routes(self)
        self.log = logging.getLogger("KarapaceAll")

    def close(self):
        # pylint: disable=bare-except
        for cls in [KarapaceBase, KafkaRest, KarapaceSchemaRegistry]:
            try:
                cls.close(self)
            except:
                pass
        try:
            self.loop.run_until_complete(KafkaRest.close_producers(self))
        except:
            pass


def main():
    parser = argparse.ArgumentParser(prog="karapace", description="Karapace: Your Kafka essentials in one tool")
    parser.add_argument("--version", action="version", help="show program version", version=karapace_version.__version__)
    parser.add_argument("config_file", help="configuration file path")
    arg = parser.parse_args()

    if not os.path.exists(arg.config_file):
        print("Config file: {} does not exist, exiting".format(arg.config_file))
        return 1
    app_type = _AppType.none
    loop = asyncio.get_event_loop()
    try:
        config = read_config(arg.config_file)
    except InvalidConfiguration:
        print("Config file: {} is invalid, exiting".format(arg.config_file))
        return 1
    if config["karapace_rest"] and config["karapace_registry"]:
        app_type = _AppType.both
        kc = KarapaceAll(arg.config_file, loop)
    elif config["karapace_rest"]:
        app_type = _AppType.rest
        kc = KafkaRest(arg.config_file, loop)
    elif config["karapace_registry"]:
        app_type = _AppType.registry
        kc = KarapaceSchemaRegistry(arg.config_file)
    else:
        print("Both rest and registry options are disabled, exiting")
        return 1
    print("=" * 100 + f"\nStarting {app_type}\n" + "=" * 100)
    try:
        signal_handler = graceful_killer(loop, app_type, kc)
        for s in [signal.SIGTERM, signal.SIGHUP, signal.SIGINT]:
            signal.signal(s, signal_handler)
        return kc.run(host=kc.config["host"], port=kc.config["port"])
    except Exception:  # pylint: disable-broad-except
        if kc.raven_client:
            kc.raven_client.captureException(tags={"where": "karapace"})
        raise


def graceful_killer(loop: asyncio.AbstractEventLoop, app_type: _AppType, app: KarapaceAll):
    def killer(sig, frame):  # pylint: disable=unused-argument
        # pylint: disable=bare-except
        app.close()
        if app_type is _AppType.rest:
            try:
                loop.run_until_complete(app.close_producers())
            except:
                pass
        try:
            loop.run_until_complete(app.app.shutdown())
        except:
            pass
        print("=" * 100 + "\n Karapace closed \n" + "=" * 100)
        sys.exit(0)

    return killer


if __name__ == "__main__":
    sys.exit(main())
