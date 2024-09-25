"""
karapace - Key correction

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from enum import Enum
from karapace.typing import ArgJsonObject
from karapace.utils import json_encode
from typing import Optional

SCHEMA_KEY_ORDER = ["keytype", "subject", "version", "magic"]
CONFIG_KEY_ORDER = ["keytype", "subject", "magic"]
NOOP_KEY_ORDER = ["keytype", "magic"]

CANONICAL_KEY_ORDERS = [SCHEMA_KEY_ORDER, CONFIG_KEY_ORDER, NOOP_KEY_ORDER]


class KeyMode(Enum):
    """Key modes supported by Karapace.

    CANONICAL format is the format that Confluent Schema Registry produces.

    DEPRECATED_KARAPACE format does not alter the order of JSON key properties. This is the deprecated
    behaviour and is not bit perfect replication of key format used by Confluent Schema Registry.
    """

    CANONICAL = 1
    DEPRECATED_KARAPACE = 2


class KeyFormatter:
    """Schema record key formatter.

    Prefer the canonical format on new installations and migrations from Confluent Schema Registry.

    Prefer Karapace key format on installations where this key format use is detected. This applies to
    Karapace installations and installations that have migrated from Confluent Schema Registry to Karapace
    and have mixed key formats in schemas topic.
    """

    def __init__(self) -> None:
        self._keymode = KeyMode.CANONICAL

    def set_keymode(self, keymode: KeyMode) -> None:
        self._keymode = keymode

    def get_keymode(self) -> KeyMode:
        return self._keymode

    def format_key(
        self,
        key: ArgJsonObject,
        keymode: Optional[KeyMode] = None,
    ) -> bytes:
        """Format key by the given keymode.

        :param key Key data as JsonData dict
        :param keymode Key mode for selecting the format. Defaults to KeyMode.CANONICAL.
        """
        keymode = keymode or self._keymode
        if keymode == KeyMode.DEPRECATED_KARAPACE:
            # No alterations
            return json_encode(key, binary=True, sort_keys=False, compact=True)
        corrected_key = {
            "keytype": key["keytype"],
        }
        if "subject" in key:
            corrected_key["subject"] = key["subject"]
        if "version" in key:
            corrected_key["version"] = key["version"]
        # Magic is the last element
        corrected_key["magic"] = key["magic"]
        return json_encode(corrected_key, binary=True, sort_keys=False, compact=True)


def is_key_in_canonical_format(key: ArgJsonObject) -> bool:
    return list(key.keys()) in CANONICAL_KEY_ORDERS
