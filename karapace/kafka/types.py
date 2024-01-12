"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from confluent_kafka import TIMESTAMP_CREATE_TIME, TIMESTAMP_LOG_APPEND_TIME, TIMESTAMP_NOT_AVAILABLE
from typing import Final

import enum

# A constant that corresponds to the default value of request.timeout.ms in
# the librdkafka C library
DEFAULT_REQUEST_TIMEOUT_MS: Final = 30000


class Timestamp(enum.IntEnum):
    NOT_AVAILABLE = TIMESTAMP_NOT_AVAILABLE
    CREATE_TIME = TIMESTAMP_CREATE_TIME
    LOG_APPEND_TIME = TIMESTAMP_LOG_APPEND_TIME
