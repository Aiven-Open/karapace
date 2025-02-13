"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

from dependency_injector import containers, providers
from karapace.core.container import KarapaceContainer
from karapace.core.instrumentation.meter import Meter
from karapace.core.stats import StatsClient


class MetricsContainer(containers.DeclarativeContainer):
    karapace_container = providers.Container(KarapaceContainer)
    meter = providers.Singleton(Meter)
    stats = providers.Singleton(StatsClient, config=karapace_container.config, meter=meter)
