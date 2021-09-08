from contextlib import closing
from karapace import version as karapace_version
from karapace.config import DEFAULT_LOG_FORMAT_JOURNAL, read_config
from karapace.kafka_rest_apis import KafkaRest

import argparse
import logging
import sys


def main() -> int:
    parser = argparse.ArgumentParser(prog="karapace rest", description="Karapace: Your Kafka essentials in one tool")
    parser.add_argument("--version", action="version", help="show program version", version=karapace_version.__version__)
    parser.add_argument("config_file", help="configuration file path", type=argparse.FileType())
    arg = parser.parse_args()

    with closing(arg.config_file):
        config = read_config(arg.config_file)

    logging.basicConfig(level=logging.INFO, format=DEFAULT_LOG_FORMAT_JOURNAL)
    logging.getLogger().setLevel(config["log_level"])
    kc = KafkaRest(config=config)
    try:
        kc.run(host=kc.config["host"], port=kc.config["port"])
    except Exception:  # pylint: disable-broad-except
        if kc.raven_client:
            kc.raven_client.captureException(tags={"where": "karapace_rest"})
        raise

    return 0


if __name__ == "__main__":
    sys.exit(main())
