"""
karapace - schema compatibility checking

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from enum import Enum, unique

import logging

LOG = logging.getLogger(__name__)


@unique
class CompatibilityModes(Enum):
    """Supported compatibility modes.

    - none: no compatibility checks done.
    - backward compatibility: new schema can *read* data produced by the olders
      schemas.
    - forward compatibility: new schema can *produce* data compatible with old
      schemas.
    - transitive compatibility: new schema can read data produced by *all*
      previous schemas, otherwise only the previous schema is checked.
    """

    BACKWARD = "BACKWARD"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD = "FORWARD"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL = "FULL"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"
    NONE = "NONE"

    def is_transitive(self) -> bool:
        TRANSITIVE_MODES = {
            "BACKWARD_TRANSITIVE",
            "FORWARD_TRANSITIVE",
            "FULL_TRANSITIVE",
        }
        return self.value in TRANSITIVE_MODES
