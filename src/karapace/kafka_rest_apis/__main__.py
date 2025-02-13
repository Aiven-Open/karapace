"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from dependency_injector.wiring import inject, Provide
from karapace import version as karapace_version
from karapace.core.config import Config
from karapace.core.container import KarapaceContainer
from karapace.core.instrumentation.prometheus import PrometheusInstrumentation
from karapace.core.logging_setup import configure_logging, log_config_without_secrets
from karapace.kafka_rest_apis import KafkaRest

import argparse
import logging
import sys


@inject
def main(
    config: Config = Provide[KarapaceContainer.config],
    prometheus: PrometheusInstrumentation = Provide[KarapaceContainer.prometheus],
) -> int:
    config.set_config_defaults()
    parser = argparse.ArgumentParser(prog="karapace", description="Karapace: Your Kafka essentials in one tool")
    parser.add_argument("--version", action="version", help="show program version", version=karapace_version.__version__)
    parser.parse_args()
    configure_logging(config=config)
    log_config_without_secrets(config=config)

    logging.info("\n%s\nStarting %s\n%s", ("=" * 100), "Starting Karapace Rest Proxy", ("=" * 100))
    app = KafkaRest(config=config)

    try:
        prometheus.setup_metrics(app=app)
        app.run()  # `close` will be called by the callback `close_by_app` set by `KarapaceBase`
    except Exception as ex:
        app.stats.unexpected_exception(ex=ex, where="karapace")
        raise
    return 0


if __name__ == "__main__":
    container = KarapaceContainer()
    container.wire(modules=[__name__])
    sys.exit(main())
