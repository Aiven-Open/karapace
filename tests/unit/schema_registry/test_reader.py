"""
karapace - Test schema reader

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from _pytest.logging import LogCaptureFixture
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from confluent_kafka import Message
from dataclasses import dataclass
from karapace.core.container import KarapaceContainer
from karapace.core.errors import CorruptKafkaRecordException, ShutdownException
from karapace.core.in_memory_database import InMemoryDatabase
from karapace.core.kafka.consumer import KafkaConsumer
from karapace.core.key_format import KeyFormatter
from karapace.core.offset_watcher import OffsetWatcher
from karapace.core.schema_type import SchemaType
from karapace.core.typing import SchemaId, Version
from pytest import MonkeyPatch
from schema_registry.reader import (
    KafkaSchemaReader,
    MAX_MESSAGES_TO_CONSUME_AFTER_STARTUP,
    MAX_MESSAGES_TO_CONSUME_ON_STARTUP,
    MessageType,
    OFFSET_EMPTY,
    OFFSET_UNINITIALIZED,
)
from tests.base_testcase import BaseTestCase
from tests.utils import schema_protobuf_invalid_because_corrupted, schema_protobuf_with_invalid_ref
from unittest.mock import Mock

import confluent_kafka
import json
import logging
import pytest
import random
import time


def test_offset_watcher() -> None:
    watcher = OffsetWatcher()
    timeout = 0.5

    # A largish number of iteration useful to stress the code
    total_number_of_offsets = 100

    # A random sleep is added after every iteration of both the consumer and
    # the producer, the goal is to simulate race conditions were the producer
    # will see an event first,  even though the consumer is more likely of
    # doing so.
    max_sleep = 0.01

    assert timeout > max_sleep, "Bad configuration, test will fail."
    assert max_sleep * total_number_of_offsets < 5, "Bad configuration, test would be too slow."

    consumed_cnt = 0

    def consume() -> None:
        nonlocal consumed_cnt
        for offset in range(total_number_of_offsets):
            assert watcher.wait_for_offset(expected_offset=offset, timeout=timeout), "Event must be produced."
            consumed_cnt += 1
            sleep = random.uniform(0, max_sleep)
            time.sleep(sleep)

    produced_cnt = 0

    def produce() -> None:
        nonlocal produced_cnt
        for offset in range(total_number_of_offsets):
            watcher.offset_seen(new_offset=offset)
            produced_cnt += 1
            sleep = random.uniform(0, max_sleep)
            time.sleep(sleep)

    with ThreadPoolExecutor(max_workers=2) as executor:
        consumer = executor.submit(consume)
        producer = executor.submit(produce)
        assert consumer.result() is None, "Thread should finish without errors"
        assert producer.result() is None, "Thread should finish without errors"

    assert watcher._greatest_offset == 99, "Expected greatest offset is not one less than total count"
    assert produced_cnt == 100, "Did not produce expected amount of records"
    assert consumed_cnt == 100, "Did not consume expected amount of records"


@dataclass
class ReadinessTestCase(BaseTestCase):
    cur_offset: int
    end_offset: int
    expected: bool


@pytest.mark.parametrize(
    "testcase",
    [
        ReadinessTestCase(
            test_name="Empty schemas topic",
            cur_offset=OFFSET_EMPTY,
            end_offset=0,
            expected=True,
        ),
        ReadinessTestCase(
            test_name="Schema topic with data, beginning offset is 0",
            cur_offset=OFFSET_EMPTY,
            end_offset=100,
            expected=False,
        ),
        ReadinessTestCase(
            test_name="Schema topic with single record",
            cur_offset=OFFSET_EMPTY,
            end_offset=1,
            expected=False,
        ),
        ReadinessTestCase(
            test_name="Beginning offset cannot be resolved.",
            cur_offset=OFFSET_UNINITIALIZED,
            end_offset=0,
            expected=False,
        ),
        ReadinessTestCase(
            test_name="Purged/compacted schemas topic, begin offset n > 0, end offset n+1",
            cur_offset=90,
            end_offset=91,
            expected=True,
        ),
        ReadinessTestCase(
            test_name="Schema topic with single record and replayed",
            cur_offset=0,
            end_offset=0,
            expected=True,
        ),
        ReadinessTestCase(
            test_name="Schema topic with data but compacted or purged, cur offset 10",
            cur_offset=10,
            end_offset=100,
            expected=False,
        ),
        ReadinessTestCase(
            test_name="Schema topic with data, cur offset is highest",
            cur_offset=99,
            end_offset=100,
            expected=True,
        ),
        ReadinessTestCase(
            test_name="Schema topic with data, cur offset is greater than highest",
            cur_offset=101,
            end_offset=100,
            expected=True,
        ),
    ],
)
def test_readiness_check(testcase: ReadinessTestCase, karapace_container: KarapaceContainer) -> None:
    key_formatter_mock = Mock()
    consumer_mock = Mock()
    consumer_mock.consume.return_value = []
    # Return tuple (beginning, end), end offset is the next upcoming record offset
    consumer_mock.get_watermark_offsets.return_value = (0, testcase.end_offset)

    offset_watcher = OffsetWatcher()
    schema_reader = KafkaSchemaReader(
        config=karapace_container.config(),
        offset_watcher=offset_watcher,
        key_formatter=key_formatter_mock,
        master_coordinator=None,
        database=InMemoryDatabase(),
    )
    schema_reader.consumer = consumer_mock
    schema_reader.offset = testcase.cur_offset

    schema_reader.handle_messages()
    assert schema_reader.ready() is testcase.expected


def test_num_max_messages_to_consume_moved_to_one_after_ready(karapace_container: KarapaceContainer) -> None:
    key_formatter_mock = Mock()
    consumer_mock = Mock()
    consumer_mock.consume.return_value = []
    # Return tuple (beginning, end), end offset is the next upcoming record offset
    consumer_mock.get_watermark_offsets.return_value = (0, 1)

    offset_watcher = OffsetWatcher()
    schema_reader = KafkaSchemaReader(
        config=karapace_container.config(),
        offset_watcher=offset_watcher,
        key_formatter=key_formatter_mock,
        master_coordinator=None,
        database=InMemoryDatabase(),
    )
    schema_reader.consumer = consumer_mock
    schema_reader.offset = 0
    assert schema_reader.max_messages_to_process == MAX_MESSAGES_TO_CONSUME_ON_STARTUP

    schema_reader.handle_messages()
    assert schema_reader.ready() is True
    assert schema_reader.max_messages_to_process == MAX_MESSAGES_TO_CONSUME_AFTER_STARTUP


def test_schema_reader_can_end_to_ready_state_if_last_message_is_invalid_in_schemas_topic(
    karapace_container: KarapaceContainer,
) -> None:
    key_formatter_mock = Mock(spec=KeyFormatter)
    consumer_mock = Mock(spec=KafkaConsumer)

    schema_str = json.dumps(
        {"name": "init", "type": "record", "fields": [{"name": "inner", "type": ["string", "int"]}]}
    ).encode()

    ok1_message = Mock(spec=Message)
    ok1_message.key.return_value = b'{"keytype":"SCHEMA","subject1":"test","version":1,"magic":1}'
    ok1_message.error.return_value = None
    ok1_message.value.return_value = schema_str
    ok1_message.offset.return_value = 1
    invalid_key_message = Mock(spec=Message)
    invalid_key_message.key.return_value = b"invalid-key"
    invalid_key_message.error.return_value = None
    invalid_key_message.value.return_value = schema_str
    invalid_key_message.offset.return_value = 2
    invalid_value_message = Mock(spec=Message)
    invalid_value_message.key.return_value = b'{"keytype":"SCHEMA","subject3":"test","version":1,"magic":1}'
    invalid_value_message.error.return_value = None
    invalid_value_message.value.return_value = "invalid-value"
    invalid_value_message.offset.return_value = 3

    consumer_mock.consume.side_effect = [ok1_message], [invalid_key_message], [invalid_value_message], []
    # Return tuple (beginning, end), end offset is the next upcoming record offset
    consumer_mock.get_watermark_offsets.return_value = (0, 4)

    offset_watcher = OffsetWatcher()
    schema_reader = KafkaSchemaReader(
        config=karapace_container.config(),
        offset_watcher=offset_watcher,
        key_formatter=key_formatter_mock,
        master_coordinator=None,
        database=InMemoryDatabase(),
    )
    schema_reader.consumer = consumer_mock
    schema_reader.offset = 0
    assert schema_reader.max_messages_to_process == MAX_MESSAGES_TO_CONSUME_ON_STARTUP

    schema_reader.handle_messages()
    assert schema_reader.offset == 1
    assert schema_reader.ready() is False
    schema_reader.handle_messages()
    assert schema_reader.offset == 2
    assert schema_reader.ready() is False
    schema_reader.handle_messages()
    assert schema_reader.offset == 3
    assert schema_reader.ready() is False
    schema_reader.handle_messages()  # call last time to call _is_ready()
    assert schema_reader.offset == 3
    assert schema_reader.ready() is True
    assert schema_reader.max_messages_to_process == MAX_MESSAGES_TO_CONSUME_AFTER_STARTUP


def test_soft_deleted_schema_storing(karapace_container: KarapaceContainer) -> None:
    """This tests a case when _schemas has been compacted and only
    the soft deleted version of the schema is present.
    """
    key_formatter_mock = Mock(spec=KeyFormatter)
    consumer_mock = Mock(spec=KafkaConsumer)
    soft_deleted_schema_record = Mock(spec=confluent_kafka.Message)
    soft_deleted_schema_record.error.return_value = None
    soft_deleted_schema_record.key.return_value = json.dumps(
        {
            "keytype": "SCHEMA",
            "subject": "soft-delete-test",
            "version": 1,
            "magic": 0,
        }
    )
    soft_deleted_schema_record.value.return_value = json.dumps(
        {
            "deleted": True,
            "id": 1,
            "schema": '"int"',
            "subject": "test-soft-delete-test",
            "version": 1,
        }
    )

    consumer_mock.consume.return_value = [soft_deleted_schema_record]
    # Return tuple (beginning, end), end offset is the next upcoming record offset
    consumer_mock.get_watermark_offsets.return_value = (0, 1)

    offset_watcher = OffsetWatcher()
    schema_reader = KafkaSchemaReader(
        config=karapace_container.config(),
        offset_watcher=offset_watcher,
        key_formatter=key_formatter_mock,
        master_coordinator=None,
        database=InMemoryDatabase(),
    )
    schema_reader.consumer = consumer_mock
    schema_reader.offset = 0

    schema_reader.handle_messages()

    soft_deleted_stored_schema = schema_reader.database.find_schema(schema_id=SchemaId(1))
    assert soft_deleted_stored_schema is not None


def test_handle_msg_delete_subject_logs(caplog: LogCaptureFixture, karapace_container: KarapaceContainer) -> None:
    database_mock = Mock(spec=InMemoryDatabase)
    database_mock.find_subject.return_value = True
    database_mock.find_subject_schemas.return_value = {
        Version(1): "SchemaVersion"
    }  # `SchemaVersion` is an actual object, simplified for test
    schema_reader = KafkaSchemaReader(
        config=karapace_container.config(),
        offset_watcher=OffsetWatcher(),
        key_formatter=KeyFormatter(),
        master_coordinator=None,
        database=database_mock,
    )

    with caplog.at_level(logging.WARNING, logger="schema_registry.reader"):
        schema_reader._handle_msg_schema_hard_delete(key={"subject": "test-subject", "version": 2})
        for log in caplog.records:
            assert log.name == "schema_registry.reader"
            assert log.levelname == "WARNING"
            assert log.message == "Hard delete: version: Version(2) for subject: 'test-subject' did not exist, should have"


@dataclass
class HealthCheckTestCase(BaseTestCase):
    current_time: float
    consecutive_unexpected_errors: int
    consecutive_unexpected_errors_start: float
    healthy: bool
    check_topic_error: Exception | None = None


@pytest.mark.parametrize(
    "testcase",
    [
        HealthCheckTestCase(
            test_name="No errors",
            current_time=0,
            consecutive_unexpected_errors=0,
            consecutive_unexpected_errors_start=0,
            healthy=True,
        ),
        HealthCheckTestCase(
            test_name="10 errors in 5 seconds",
            current_time=5,
            consecutive_unexpected_errors=10,
            consecutive_unexpected_errors_start=0,
            healthy=True,
        ),
        HealthCheckTestCase(
            test_name="1 error in 20 seconds",
            current_time=20,
            consecutive_unexpected_errors=1,
            consecutive_unexpected_errors_start=0,
            healthy=True,
        ),
        HealthCheckTestCase(
            test_name="3 errors in 10 seconds",
            current_time=10,
            consecutive_unexpected_errors=3,
            consecutive_unexpected_errors_start=0,
            healthy=False,
        ),
        HealthCheckTestCase(
            test_name="check topic error",
            current_time=5,
            consecutive_unexpected_errors=1,
            consecutive_unexpected_errors_start=0,
            healthy=False,
            check_topic_error=Exception("Somethings wrong"),
        ),
    ],
)
async def test_schema_reader_health_check(
    testcase: HealthCheckTestCase, monkeypatch: MonkeyPatch, karapace_container: KarapaceContainer
) -> None:
    offset_watcher = OffsetWatcher()
    key_formatter_mock = Mock()
    admin_client_mock = Mock()

    emtpy_future = Future()
    if testcase.check_topic_error:
        emtpy_future.set_exception(testcase.check_topic_error)
    else:
        emtpy_future.set_result(None)
    admin_client_mock.describe_topics.return_value = {karapace_container.config().topic_name: emtpy_future}

    schema_reader = KafkaSchemaReader(
        config=karapace_container.config(),
        offset_watcher=offset_watcher,
        key_formatter=key_formatter_mock,
        master_coordinator=None,
        database=InMemoryDatabase(),
    )

    monkeypatch.setattr(time, "monotonic", lambda: testcase.current_time)
    schema_reader.admin_client = admin_client_mock
    schema_reader.consecutive_unexpected_errors = testcase.consecutive_unexpected_errors
    schema_reader.consecutive_unexpected_errors_start = testcase.consecutive_unexpected_errors_start

    assert await schema_reader.is_healthy() == testcase.healthy


@dataclass
class KafkaMessageHandlingErrorTestCase(BaseTestCase):
    key: bytes
    value: bytes
    schema_type: SchemaType
    message_type: MessageType
    expected_error: ShutdownException
    expected_log_message: str


@pytest.fixture(name="schema_reader_with_consumer_messages_factory")
def fixture_schema_reader_with_consumer_messages_factory(
    karapace_container: KarapaceContainer,
) -> Callable[[tuple[list[Message]]], KafkaSchemaReader]:
    def factory(consumer_messages: tuple[list[Message]]) -> KafkaSchemaReader:
        key_formatter_mock = Mock(spec=KeyFormatter)
        consumer_mock = Mock(spec=KafkaConsumer)

        consumer_mock.consume.side_effect = consumer_messages
        # Return tuple (beginning, end), end offset is the next upcoming record offset
        consumer_mock.get_watermark_offsets.return_value = (0, 4)

        # Update the config to run the schema reader in strict mode so errors can be raised
        config = karapace_container.config().set_config_defaults({"kafka_schema_reader_strict_mode": True})

        offset_watcher = OffsetWatcher()
        schema_reader = KafkaSchemaReader(
            config=config,
            offset_watcher=offset_watcher,
            key_formatter=key_formatter_mock,
            master_coordinator=None,
            database=InMemoryDatabase(),
        )
        schema_reader.consumer = consumer_mock
        schema_reader.offset = 0
        assert schema_reader.max_messages_to_process == MAX_MESSAGES_TO_CONSUME_ON_STARTUP
        return schema_reader

    return factory


@pytest.fixture(name="message_factory")
def fixture_message_factory() -> Callable[[bytes, bytes, int], Message]:
    def factory(key: bytes, value: bytes, offset: int = 1) -> Message:
        message = Mock(spec=Message)
        message.key.return_value = key
        message.value.return_value = value
        message.offset.return_value = offset
        message.error.return_value = None
        return message

    return factory


@pytest.mark.parametrize(
    "test_case",
    [
        KafkaMessageHandlingErrorTestCase(
            test_name="Message key is not valid JSON",
            key=b'{subject1::::"test""version":1"magic":1}',
            value=b'{"value": "value does not matter at this stage, just correct JSON"}',
            schema_type=None,
            message_type=MessageType.schema,
            expected_error=CorruptKafkaRecordException,
            expected_log_message='Invalid JSON in msg.key(): {subject1::::"test""version":1"magic":1} at offset 1',
        ),
        KafkaMessageHandlingErrorTestCase(
            test_name="Message key is empty, i.e. `null/None`",
            key=None,
            value=b'{"value": "value does not matter at this stage, just correct JSON"}',
            schema_type=None,
            message_type=MessageType.schema,
            expected_error=CorruptKafkaRecordException,
            expected_log_message="Empty msg.key() at offset 1",
        ),
        KafkaMessageHandlingErrorTestCase(
            test_name="Keytype is missing from message key",
            key=b'{"subject":"test","version":1,"magic":1}',
            value=b'{"value": "value does not matter at this stage, just correct JSON"}',
            schema_type=None,
            message_type=MessageType.schema,
            expected_error=CorruptKafkaRecordException,
            expected_log_message=(
                "The message {'subject': 'test', 'version': 1, 'magic': 1}-"
                "{'value': 'value does not matter at this stage, just correct JSON'} "
                "has been discarded because doesn't contain the `keytype` key in the key"
            ),
        ),
        KafkaMessageHandlingErrorTestCase(
            test_name="Keytype is invalid on message key",
            key=b'{"keytype":"NOT_A_VALID_KEY_TYPE","subject":"test","version":1,"magic":1}',
            value=b'{"value": "value does not matter at this stage, just correct JSON"}',
            schema_type=None,
            message_type=None,
            expected_error=CorruptKafkaRecordException,
            expected_log_message=(
                "The message {'keytype': 'NOT_A_VALID_KEY_TYPE', 'subject': 'test', 'version': 1, 'magic': 1}-"
                "{'value': 'value does not matter at this stage, just correct JSON'} "
                "has been discarded because the NOT_A_VALID_KEY_TYPE is not managed"
            ),
        ),
        KafkaMessageHandlingErrorTestCase(
            test_name="Config message value is not valid JSON",
            key=b'{"keytype":"CONFIG","subject":null,"magic":0}',
            value=(b'no-valid-jason"compatibilityLevel": "BACKWARD""'),
            schema_type=None,
            message_type=MessageType.config,
            expected_error=CorruptKafkaRecordException,
            expected_log_message="Invalid JSON in msg.value() at offset 1",
        ),
        KafkaMessageHandlingErrorTestCase(
            test_name="Config message value is not valid config setting",
            key=b'{"keytype":"CONFIG","subject":null,"magic":0}',
            value=b'{"not_the_key_name":"INVALID_CONFIG"}',
            schema_type=None,
            message_type=MessageType.config,
            expected_error=CorruptKafkaRecordException,
            expected_log_message=(
                "The message {'keytype': 'CONFIG', 'subject': None, 'magic': 0}-"
                "{'not_the_key_name': 'INVALID_CONFIG'} has been discarded because the CONFIG is not managed"
            ),
        ),
        KafkaMessageHandlingErrorTestCase(
            test_name="Version in schema message value is not valid",
            key=b'{"keytype":"SCHEMA","subject":"test","version":1,"magic":1}',
            value=(
                b'{"subject": "test", "version": "invalid-version", "id": 1, "deleted": false,'
                b'"schema": "{\\"name\\": \\"test\\", \\"type\\": \\"record\\", \\"fields\\": '
                b'[{\\"name\\": \\"test_field\\", \\"type\\": [\\"string\\", \\"int\\"]}]}"}'
            ),
            schema_type=SchemaType.AVRO,
            message_type=MessageType.schema,
            expected_error=CorruptKafkaRecordException,
            expected_log_message=(
                "The message {'keytype': 'SCHEMA', 'subject': 'test', 'version': 1, 'magic': 1}-"
                "{'subject': 'test', 'version': 'invalid-version', 'id': 1, 'deleted': False, 'schema': "
                '\'{"name": "test", "type": "record", "fields": [{"name": "test_field", "type": ["string", "int"]}]}\'} '
                "has been discarded because the SCHEMA is not managed"
            ),
        ),
        KafkaMessageHandlingErrorTestCase(
            test_name="Message value is not valid JSON",
            key=b'{"keytype":"SCHEMA","subject":"test","version":1,"magic":1}',
            value=(
                b'no-valid-json"version": 1, "id": 1, "deleted": false,'
                b'"schema": "{\\"name\\": \\"test\\", \\"type\\": \\"record\\", \\"fields\\": '
                b'[{\\"name\\": \\"test_field\\", \\"type\\": [\\"string\\", \\"int\\"]}]}"}'
            ),
            schema_type=SchemaType.AVRO,
            message_type=MessageType.schema,
            expected_error=CorruptKafkaRecordException,
            expected_log_message="Invalid JSON in msg.value() at offset 1",
        ),
        KafkaMessageHandlingErrorTestCase(
            test_name="Delete subject message value is missing `subject` field",
            key=b'{"keytype":"DELETE_SUBJECT","subject":"test","version":1,"magic":1}',
            value=b'{"not-subject-key":"test","version":1}',
            schema_type=None,
            message_type=MessageType.delete_subject,
            expected_error=CorruptKafkaRecordException,
            expected_log_message=(
                "The message {'keytype': 'DELETE_SUBJECT', 'subject': 'test', 'version': 1, 'magic': 1}-"
                "{'not-subject-key': 'test', 'version': 1} has been discarded because the DELETE_SUBJECT is not managed"
            ),
        ),
        KafkaMessageHandlingErrorTestCase(
            test_name="Protobuf schema is invalid",
            key=b'{"keytype":"SCHEMA","subject":"test","version":1,"magic":1}',
            value=(
                b'{"schemaType": "PROTOBUF", "subject": "test", "version": 1, "id": 1, "deleted": false, "schema":'
                + json.dumps(schema_protobuf_invalid_because_corrupted).encode()
                + b"}"
            ),
            schema_type=SchemaType.PROTOBUF,
            message_type=MessageType.schema,
            expected_error=CorruptKafkaRecordException,
            expected_log_message="Schema is not valid ProtoBuf definition",
        ),
    ],
)
def test_message_error_handling(
    caplog: LogCaptureFixture,
    test_case: KafkaMessageHandlingErrorTestCase,
    schema_reader_with_consumer_messages_factory: Callable[[tuple[list[Message]]], KafkaSchemaReader],
    message_factory: Callable[[bytes, bytes, int], Message],
) -> None:
    message = message_factory(key=test_case.key, value=test_case.value)
    consumer_messages = ([message],)
    schema_reader = schema_reader_with_consumer_messages_factory(consumer_messages)

    with caplog.at_level(logging.WARNING, logger="schema_registry.reader"):
        with pytest.raises(test_case.expected_error):
            schema_reader.handle_messages()

        assert schema_reader.offset == 1
        assert not schema_reader.ready()
        for log in caplog.records:
            assert log.name == "schema_registry.reader"
            assert log.levelname == "WARNING"
            assert log.message == test_case.expected_log_message


def test_message_error_handling_with_invalid_reference_schema_protobuf(
    caplog: LogCaptureFixture,
    schema_reader_with_consumer_messages_factory: Callable[[tuple[list[Message]]], KafkaSchemaReader],
    message_factory: Callable[[bytes, bytes, int], Message],
) -> None:
    # Given an invalid schema (corrupted)
    key_ref = b'{"keytype":"SCHEMA","subject":"testref","version":1,"magic":1}'
    value_ref = (
        b'{"schemaType": "PROTOBUF", "subject": "testref", "version": 1, "id": 1, "deleted": false'
        + b', "schema": '
        + json.dumps(schema_protobuf_invalid_because_corrupted).encode()
        + b"}"
    )
    message_ref = message_factory(key=key_ref, value=value_ref)

    # And given a schema referencing that corrupted schema (valid otherwise)
    key_using_ref = b'{"keytype":"SCHEMA","subject":"test","version":1,"magic":1}'
    value_using_ref = (
        b'{"schemaType": "PROTOBUF", "subject": "test", "version": 1, "id": 1, "deleted": false'
        + b', "schema": '
        + json.dumps(schema_protobuf_with_invalid_ref).encode()
        + b', "references": [{"name": "testref.proto", "subject": "testref", "version": 1}]'
        + b"}"
    )
    message_using_ref = message_factory(key=key_using_ref, value=value_using_ref)

    with caplog.at_level(logging.WARN, logger="schema_registry.reader"):
        # When handling the corrupted schema
        schema_reader = schema_reader_with_consumer_messages_factory(([message_ref],))

        # Then the schema is recognised as invalid
        with pytest.raises(CorruptKafkaRecordException):
            schema_reader.handle_messages()

            assert schema_reader.offset == 1
            assert not schema_reader.ready()

        # When handling the schema
        schema_reader.consumer.consume.side_effect = ([message_using_ref],)

        # Then the schema is recognised as invalid because of the corrupted referenced schema
        with pytest.raises(CorruptKafkaRecordException):
            schema_reader.handle_messages()

            assert schema_reader.offset == 1
            assert not schema_reader.ready()

        warn_records = [r for r in caplog.records if r.levelname == "WARNING"]

        assert len(warn_records) == 2

        # Check that different warnings are logged for each schema
        assert warn_records[0].name == "schema_registry.reader"
        assert warn_records[0].message == "Schema is not valid ProtoBuf definition"

        assert warn_records[1].name == "schema_registry.reader"
        assert warn_records[1].message == "Invalid Protobuf references"
