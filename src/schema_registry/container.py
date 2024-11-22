"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector import containers, providers
from dependency_injector.wiring import Provide, inject

from karapace.config import Config


class SchemaRegistryContainer(containers.DeclarativeContainer):
    base_config = providers.Configuration()
    config = providers.Singleton(
        Config,
        _env_file=base_config.karapace.env_file,
        _env_file_encoding=base_config.karapace.env_file_encoding,
    )
