"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""


from fastapi import Depends
from schema_registry.dependencies.config_dependency import ConfigDependencyManager
from karapace.statsd import StatsClient
from typing import Annotated


class StatsDependencyManager:
    STATS_CLIENT: StatsClient | None = None

    @classmethod
    def get_stats(cls) -> StatsClient:
        if not StatsDependencyManager.STATS_CLIENT:
            StatsDependencyManager.STATS_CLIENT = StatsClient(config=ConfigDependencyManager.get_config())
        return StatsDependencyManager.STATS_CLIENT


StatsDep = Annotated[StatsClient, Depends(StatsDependencyManager.get_stats)]
