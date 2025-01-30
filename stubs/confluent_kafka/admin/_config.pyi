from enum import Enum
from typing import cast

from ._resource import ResourceType

class ConfigResource:
    Type = ResourceType

    def __init__(
        self,
        restype: ResourceType,
        name: str,
        set_config: dict[str, str] | None = None,
    ) -> None: ...

class ConfigSource(Enum):
    UNKNOWN_CONFIG = cast(int, ...)
    DYNAMIC_TOPIC_CONFIG = cast(int, ...)

class ConfigEntry: ...
