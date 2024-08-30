"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from _pytest.logging import LogCaptureFixture
from karapace.errors import CorruptKafkaRecordException
from karapace.kafka_error_handler import KafkaErrorHandler, KafkaErrorLocation

import aiokafka.errors as Errors
import logging
import pytest


@pytest.fixture(name="kafka_error_handler")
def fixture_kafka_error_handler() -> KafkaErrorHandler:
    config = {
        "kafka_schema_reader_strict_mode": False,
        "kafka_retriable_errors_silenced": True,
    }
    return KafkaErrorHandler(config=config)


@pytest.mark.parametrize(
    "retriable_error",
    [
        Errors.NodeNotReadyError("node is still starting"),
        Errors.GroupCoordinatorNotAvailableError("group is unavailable"),
        Errors.NoBrokersAvailable("no brokers available"),
    ],
)
def test_handle_error_retriable_schema_coordinator(
    caplog: LogCaptureFixture,
    kafka_error_handler: KafkaErrorHandler,
    retriable_error: Errors.KafkaError,
):
    kafka_error_handler.retriable_errors_silenced = True
    with caplog.at_level(logging.WARNING, logger="karapace.error_handler"):
        kafka_error_handler.handle_error(location=KafkaErrorLocation.SCHEMA_COORDINATOR, error=retriable_error)

        for log in caplog.records:
            assert log.name == "karapace.kafka_error_handler"
            assert log.levelname == "WARNING"
            assert log.message == f"SCHEMA_COORDINATOR encountered error - {retriable_error}"

    # Check that the config flag - `kafka_retriable_errors_silenced` switches the behaviour
    kafka_error_handler.retriable_errors_silenced = False
    with pytest.raises(retriable_error.__class__):
        kafka_error_handler.handle_error(location=KafkaErrorLocation.SCHEMA_COORDINATOR, error=retriable_error)


@pytest.mark.parametrize(
    "nonretriable_error",
    [
        ValueError("value missing"),
        Errors.GroupAuthorizationFailedError("authorization failed"),
        Errors.InvalidCommitOffsetSizeError("invalid commit size"),
    ],
)
def test_handle_error_nonretriable_schema_coordinator(
    kafka_error_handler: KafkaErrorHandler, nonretriable_error: BaseException
) -> None:
    kafka_error_handler.retriable_errors_silenced = False
    with pytest.raises(nonretriable_error.__class__):
        kafka_error_handler.handle_error(location=KafkaErrorLocation.SCHEMA_COORDINATOR, error=nonretriable_error)

    # Check that the config flag - `kafka_retriable_errors_silenced` switches the behaviour
    kafka_error_handler.retriable_errors_silenced = True
    kafka_error_handler.handle_error(location=KafkaErrorLocation.SCHEMA_COORDINATOR, error=nonretriable_error)


def test_handle_error_schema_reader(kafka_error_handler: KafkaErrorHandler) -> None:
    kafka_error_handler.schema_reader_strict_mode = True
    with pytest.raises(CorruptKafkaRecordException):
        kafka_error_handler.handle_error(location=KafkaErrorLocation.SCHEMA_READER, error=Exception)

    # Check that the config flag - `kafka_schema_reader_strict_mode` switches the behaviour
    kafka_error_handler.schema_reader_strict_mode = False
    kafka_error_handler.handle_error(location=KafkaErrorLocation.SCHEMA_READER, error=Exception)
