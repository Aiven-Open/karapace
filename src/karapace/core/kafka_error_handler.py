"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from karapace.core.config import Config
from karapace.core.errors import CorruptKafkaRecordException
from karapace.core.typing import StrEnum

import aiokafka.errors as Errors
import enum
import logging

LOG = logging.getLogger(__name__)


class KafkaErrorLocation(StrEnum):
    SCHEMA_COORDINATOR = "SCHEMA_COORDINATOR"
    SCHEMA_READER = "SCHEMA_READER"


class KafkaRetriableErrors(enum.Enum):
    SCHEMA_COORDINATOR = (Errors.NodeNotReadyError,)


class KafkaErrorHandler:
    def __init__(self, config: Config) -> None:
        self.schema_reader_strict_mode: bool = config.kafka_schema_reader_strict_mode
        self.retriable_errors_silenced: bool = config.kafka_retriable_errors_silenced

    def log(self, location: KafkaErrorLocation, error: BaseException) -> None:
        LOG.warning("%s encountered error - %s", location, error)

    def handle_schema_coordinator_error(self, error: BaseException) -> None:
        if getattr(error, "retriable", False) or (
            error in KafkaRetriableErrors[KafkaErrorLocation.SCHEMA_COORDINATOR].value
        ):
            self.log(location=KafkaErrorLocation.SCHEMA_COORDINATOR, error=error)
        if not self.retriable_errors_silenced:
            raise error

    def handle_schema_reader_error(self, error: BaseException) -> None:
        if self.schema_reader_strict_mode:
            raise CorruptKafkaRecordException from error

    def handle_error(self, location: KafkaErrorLocation, error: BaseException) -> None:
        return getattr(self, f"handle_{location.lower()}_error")(error=error)
