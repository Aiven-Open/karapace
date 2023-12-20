from ._resource import ResourceType
from enum import Enum

class ConfigResource:
    Type = ResourceType

    def __init__(
        self,
        restype: ResourceType,
        name: str,
        set_config: dict[str, str] | None = None,
    ) -> None: ...

class ConfigSource(Enum):
    UNKNOWN_CONFIG: int
    DYNAMIC_TOPIC_CONFIG: int

class ConfigEntry: ...
