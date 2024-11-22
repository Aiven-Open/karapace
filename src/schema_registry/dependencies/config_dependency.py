"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import Depends
from karapace.config import Config
from typing import Annotated

import os

env_file = os.environ.get("KARAPACE_DOTENV", None)


class ConfigDependencyManager:
    CONFIG = Config(_env_file=env_file, _env_file_encoding="utf-8")

    @classmethod
    def get_config(cls) -> Config:
        return ConfigDependencyManager.CONFIG


ConfigDep = Annotated[Config, Depends(ConfigDependencyManager.get_config)]
