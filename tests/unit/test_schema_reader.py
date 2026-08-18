"""
karapace - Test schema reader

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import json
import logging
import random
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from unittest.mock import Mock, patch

import confluent_kafka
import pytest
from _pytest.logging import LogCaptureFixture
from aiokafka.errors import (
    GroupAuthorizationFailedError,
    KafkaTimeoutError,
    LeaderNotAvailableError,
    NotLeaderForPartitionError,
    TopicAuthorizationFailedError,
    UnknownTopicOrPartitionError,
)
from confluent_kafka import Message, TopicPartition
from pytest import MonkeyPatch

from karapace.core.container import KarapaceContainer
from karapace.core.errors import CorruptKafkaRecordException, InvalidReferences, InvalidSchema, ShutdownException
from karapace.core.in_memory_database import InMemoryDatabase
from karapace.core.kafka.consumer import KafkaConsumer
from karapace.core.key_format import KeyFormatter, KeyMode
from karapace.core.offset_watcher import OffsetWatcher
from karapace.core.schema_models import TypedSchema, ValidatedTypedSchema
from karapace.core.schema_reader import (
    MAX_MESSAGES_TO_CONSUME_AFTER_STARTUP,
    MAX_MESSAGES_TO_CONSUME_ON_STARTUP,
    OFFSET_EMPTY,
    OFFSET_UNINITIALIZED,
    KafkaSchemaReader,
    MessageType,
)
from karapace.core.schema_references import LatestVersionReference, Reference
from karapace.core.schema_type import SchemaType
from karapace.core.stats import StatsClient
from karapace.core.typing import PrimaryInfo, SchemaId, Subject, Version
from tests.base_testcase import BaseTestCase
from tests.utils import (
    schema_avro_json,
    schema_protobuf_invalid_because_corrupted,
    schema_protobuf_with_invalid_ref,
)


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
    key_formatter_mock = Mock(spec=KeyFormatter)
    stats_mock = Mock(spec=StatsClient)
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
        stats=stats_mock,
    )
    schema_reader.consumer = consumer_mock
    schema_reader.offset = testcase.cur_offset

    schema_reader.handle_messages()
    assert schema_reader.ready() is testcase.expected


def test_num_max_messages_to_consume_moved_to_one_after_ready(karapace_container: KarapaceContainer) -> None:
    key_formatter_mock = Mock(spec=KeyFormatter)
    stats_mock = Mock(spec=StatsClient)
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
        stats=stats_mock,
    )
    schema_reader.consumer = consumer_mock
    schema_reader.offset = 0
    assert schema_reader.max_messages_to_process == MAX_MESSAGES_TO_CONSUME_ON_STARTUP

    schema_reader.handle_messages()
    assert schema_reader.ready() is True
    assert schema_reader.max_messages_to_process == MAX_MESSAGES_TO_CONSUME_AFTER_STARTUP


def test_schema_reader_skips_empty_message_and_advances_offset(karapace_container: KarapaceContainer) -> None:
    key_formatter_mock = Mock(spec=KeyFormatter)
    stats_mock = Mock(spec=StatsClient)
    consumer_mock = Mock(spec=KafkaConsumer)

    empty_message = Mock(spec=Message)
    empty_message.key.return_value = None
    empty_message.value.return_value = None
    empty_message.error.return_value = None
    empty_message.offset.return_value = 5

    consumer_mock.consume.return_value = [empty_message]
    consumer_mock.get_watermark_offsets.return_value = (0, 6)

    schema_reader = KafkaSchemaReader(
        config=karapace_container.config(),
        offset_watcher=OffsetWatcher(),
        key_formatter=key_formatter_mock,
        master_coordinator=None,
        database=InMemoryDatabase(),
        stats=stats_mock,
    )
    schema_reader.consumer = consumer_mock
    schema_reader.offset = 0

    schema_reader.handle_messages()

    assert schema_reader.offset == 5
    schema_reader._update_is_ready_flag()  # pylint: disable=protected-access
    assert schema_reader.ready() is True


def test_schema_reader_becomes_ready_when_topic_tail_is_control_record(karapace_container: KarapaceContainer) -> None:
    """A transaction control record at the topic tail is never delivered to the
    application, so `self.offset` stalls one short of `_highest_offset`. The
    consumer's fetch position advances past it and must drive readiness.
    """
    key_formatter_mock = Mock(spec=KeyFormatter)
    stats_mock = Mock(spec=StatsClient)
    consumer_mock = Mock(spec=KafkaConsumer)

    # No deliverable messages: the last consume returns an empty batch.
    consumer_mock.consume.return_value = []
    # end_offset 22 => _highest_offset 21 (offset 21 is the commit control record)
    consumer_mock.get_watermark_offsets.return_value = (0, 22)
    # Fetch position has advanced past the control record to the end.
    consumer_mock.position.return_value = [TopicPartition("_schemas", 0, 22)]

    schema_reader = KafkaSchemaReader(
        config=karapace_container.config(),
        offset_watcher=OffsetWatcher(),
        key_formatter=key_formatter_mock,
        master_coordinator=None,
        database=InMemoryDatabase(),
        stats=stats_mock,
    )
    schema_reader.consumer = consumer_mock
    schema_reader.offset = 20  # last delivered (real) record

    schema_reader.handle_messages()

    assert schema_reader.ready() is True


def test_schema_reader_not_ready_when_position_behind_end(karapace_container: KarapaceContainer) -> None:
    """If the fetch position has not reached the end, the reader stays not ready."""
    key_formatter_mock = Mock(spec=KeyFormatter)
    stats_mock = Mock(spec=StatsClient)
    consumer_mock = Mock(spec=KafkaConsumer)

    consumer_mock.consume.return_value = []
    consumer_mock.get_watermark_offsets.return_value = (0, 22)
    # Position still behind the end offset.
    consumer_mock.position.return_value = [TopicPartition("_schemas", 0, 20)]

    schema_reader = KafkaSchemaReader(
        config=karapace_container.config(),
        offset_watcher=OffsetWatcher(),
        key_formatter=key_formatter_mock,
        master_coordinator=None,
        database=InMemoryDatabase(),
        stats=stats_mock,
    )
    schema_reader.consumer = consumer_mock
    schema_reader.offset = 20

    schema_reader.handle_messages()

    assert schema_reader.ready() is False


def test_schema_reader_position_ignored_when_batch_not_empty(karapace_container: KarapaceContainer) -> None:
    """Position must not mark the reader ready while a non-empty batch is still
    pending processing, even if the position already reached the end.
    """
    key_formatter_mock = Mock(spec=KeyFormatter)
    stats_mock = Mock(spec=StatsClient)
    consumer_mock = Mock(spec=KafkaConsumer)

    pending_message = Mock(spec=Message)
    pending_message.key.return_value = b'{"keytype":"SCHEMA","subject":"test","version":1,"magic":1}'
    pending_message.value.return_value = json.dumps(
        {"name": "init", "type": "record", "fields": [{"name": "inner", "type": ["string", "int"]}]}
    ).encode()
    pending_message.error.return_value = None
    pending_message.offset.return_value = 20

    consumer_mock.consume.return_value = [pending_message]
    consumer_mock.get_watermark_offsets.return_value = (0, 22)
    consumer_mock.position.return_value = [TopicPartition("_schemas", 0, 22)]

    schema_reader = KafkaSchemaReader(
        config=karapace_container.config(),
        offset_watcher=OffsetWatcher(),
        key_formatter=key_formatter_mock,
        master_coordinator=None,
        database=InMemoryDatabase(),
        stats=stats_mock,
    )
    schema_reader.consumer = consumer_mock
    schema_reader.offset = 19

    schema_reader.handle_messages()

    # Readiness was evaluated before the batch was processed; position is ignored
    # for a non-empty batch, so the reader is not ready yet.
    assert schema_reader.ready() is False


def test_schema_reader_can_end_to_ready_state_if_last_message_is_invalid_in_schemas_topic(
    karapace_container: KarapaceContainer,
) -> None:
    key_formatter_mock = Mock(spec=KeyFormatter)
    stats_mock = Mock(spec=StatsClient)

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
        stats=stats_mock,
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
    stats_mock = Mock(spec=StatsClient)
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
        stats=stats_mock,
    )
    schema_reader.consumer = consumer_mock
    schema_reader.offset = 0

    schema_reader.handle_messages()

    soft_deleted_stored_schema = schema_reader.database.find_schema(schema_id=SchemaId(1))
    assert soft_deleted_stored_schema is not None


def test_handle_msg_delete_subject_logs(caplog: LogCaptureFixture, karapace_container: KarapaceContainer) -> None:
    key_formatter_mock = Mock(spec=KeyFormatter)
    stats_mock = Mock(spec=StatsClient)
    database_mock = Mock(spec=InMemoryDatabase)
    database_mock.find_subject.return_value = True
    database_mock.find_subject_schemas.return_value = {
        Version(1): "SchemaVersion"
    }  # `SchemaVersion` is an actual object, simplified for test
    schema_reader = KafkaSchemaReader(
        config=karapace_container.config(),
        offset_watcher=OffsetWatcher(),
        key_formatter=key_formatter_mock,
        master_coordinator=None,
        database=database_mock,
        stats=stats_mock,
    )

    with caplog.at_level(logging.WARNING, logger="karapace.core.schema_reader"):
        schema_reader._handle_msg_schema_hard_delete(key={"subject": "test-subject", "version": 2})
        for log in caplog.records:
            assert log.name == "karapace.core.schema_reader"
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
    key_formatter_mock = Mock(spec=KeyFormatter)
    stats_mock = Mock(spec=StatsClient)
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
        stats=stats_mock,
    )

    monkeypatch.setattr(time, "monotonic", lambda: testcase.current_time)
    schema_reader.admin_client = admin_client_mock
    schema_reader.consecutive_unexpected_errors = testcase.consecutive_unexpected_errors
    schema_reader.consecutive_unexpected_errors_start = testcase.consecutive_unexpected_errors_start

    assert await schema_reader.is_healthy() == testcase.healthy


@dataclass
class KafkaMessageHandlingErrorTestCase(BaseTestCase):
    key: bytes | None
    value: bytes | None
    schema_type: SchemaType | None
    message_type: MessageType | None
    expected_error: ShutdownException
    expected_log_message: str


@pytest.fixture(name="schema_reader_with_consumer_messages_factory")
def fixture_schema_reader_with_consumer_messages_factory(
    karapace_container: KarapaceContainer,
) -> Callable[[tuple[list[Message]]], KafkaSchemaReader]:
    def factory(consumer_messages: tuple[list[Message]]) -> KafkaSchemaReader:
        key_formatter_mock = Mock(spec=KeyFormatter)
        stats_mock = Mock(spec=StatsClient)
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
            stats=stats_mock,
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

    with caplog.at_level(logging.WARNING, logger="karapace.core.schema_reader"):
        with pytest.raises(test_case.expected_error):
            schema_reader.handle_messages()

        assert schema_reader.offset == 1
        assert not schema_reader.ready()
        for log in caplog.records:
            assert log.name == "karapace.core.schema_reader"
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

    with caplog.at_level(logging.WARNING, logger="karapace.core.schema_reader"):
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
        assert warn_records[0].name == "karapace.core.schema_reader"
        assert warn_records[0].message == "Schema is not valid ProtoBuf definition"

        assert warn_records[1].name == "karapace.core.schema_reader"
        assert warn_records[1].message == "Invalid Protobuf references"


def test_message_error_handling_with_invalid_reference_schema_avro(
    caplog: LogCaptureFixture,
    schema_reader_with_consumer_messages_factory: Callable[[tuple[list[Message]]], KafkaSchemaReader],
    message_factory: Callable[[bytes, bytes, int], Message],
) -> None:
    # Given an invalid schema (malformed JSON)
    key_ref = b'{"keytype":"SCHEMA","subject":"testref","version":1,"magic":1}'
    value_ref = (
        b'{"schemaType": "AVRO", "subject": "testref", "version": 1, "id": 1, "deleted": false'
        b', "schema": "not-a-valid-json"}'
    )
    message_ref = message_factory(key=key_ref, value=value_ref)

    # And given a schema referencing that missing schema (valid otherwise)
    key_using_ref = b'{"keytype":"SCHEMA","subject":"test","version":1,"magic":1}'
    value_using_ref = (
        b'{"schemaType": "AVRO", "subject": "test", "version": 1, "id": 1, "deleted": false'
        b', "schema": '
        + schema_avro_json.encode()
        + b', "references": [{"name": "testref.avsc", "subject": "testref", "version": 1}]'
        + b"}"
    )
    message_using_ref = message_factory(key=key_using_ref, value=value_using_ref)

    with caplog.at_level(logging.WARNING, logger="karapace.core.schema_reader"):
        # When handling the corrupted schema
        schema_reader = schema_reader_with_consumer_messages_factory(([message_ref],))

        # Then the schema is recognised as invalid
        with pytest.raises(CorruptKafkaRecordException):
            schema_reader.handle_messages()

            assert schema_reader.offset == 1
            assert not schema_reader.ready()

        # When handling the schema
        schema_reader.consumer.consume.side_effect = ([message_using_ref],)

        # Then the schema is recognised as invalid because of the missing referenced schema
        with pytest.raises(CorruptKafkaRecordException):
            schema_reader.handle_messages()

            assert schema_reader.offset == 1
            assert not schema_reader.ready()

        warn_records = [r for r in caplog.records if r.levelname == "WARNING"]

        assert len(warn_records) == 2

        # Check that different warnings are logged for each schema
        assert warn_records[0].name == "karapace.core.schema_reader"
        assert warn_records[0].message == "Schema is not valid JSON"

        assert warn_records[1].name == "karapace.core.schema_reader"
        assert warn_records[1].message == "Invalid Avro references"


def _make_schema_reader(karapace_container: KarapaceContainer, **overrides) -> KafkaSchemaReader:
    kwargs = {
        "config": karapace_container.config(),
        "offset_watcher": OffsetWatcher(),
        "key_formatter": Mock(spec=KeyFormatter),
        "master_coordinator": None,
        "database": InMemoryDatabase(),
        "stats": Mock(spec=StatsClient),
    }
    kwargs.update(overrides)
    return KafkaSchemaReader(**kwargs)


def _avro_schema(name: str = "Obj") -> TypedSchema:
    return TypedSchema(schema_type=SchemaType.AVRO, schema_str=f'{{"type": "record", "name": "{name}", "fields": []}}')


class TestSimpleMethods:
    def test_close_sets_stop_event(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        assert not reader._stop_schema_reader.is_set()
        reader.close()
        assert reader._stop_schema_reader.is_set()

    def test_highest_offset_is_max_of_internal_and_watcher(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        reader._highest_offset = 5
        reader._offset_watcher.offset_seen(10)
        assert reader.highest_offset() == 10

    def test_set_not_ready_clears_ready_flag(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        reader._ready = True
        reader.set_not_ready()
        assert reader.ready() is False


class TestParseMessageValue:
    def test_dict_value_returned_as_is(self) -> None:
        assert KafkaSchemaReader._parse_message_value(json.dumps({"a": 1})) == {"a": 1}

    def test_none_value_returns_none(self) -> None:
        assert KafkaSchemaReader._parse_message_value(json.dumps(None)) is None

    def test_empty_string_value_returns_none(self) -> None:
        assert KafkaSchemaReader._parse_message_value(json.dumps("")) is None

    def test_non_dict_non_empty_value_raises_type_error(self) -> None:
        with pytest.raises(TypeError):
            KafkaSchemaReader._parse_message_value(json.dumps([1, 2, 3]))


class TestGetBeginningOffset:
    def test_returns_watermark_minus_one(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        reader.consumer = Mock(spec=KafkaConsumer)
        reader.consumer.get_watermark_offsets.return_value = (5, 100)
        assert reader._get_beginning_offset() == 4

    @pytest.mark.parametrize(
        "error",
        [KafkaTimeoutError(), UnknownTopicOrPartitionError(), LeaderNotAvailableError(), NotLeaderForPartitionError()],
    )
    def test_known_errors_return_uninitialized(self, karapace_container: KarapaceContainer, error: Exception) -> None:
        reader = _make_schema_reader(karapace_container)
        reader.consumer = Mock(spec=KafkaConsumer)
        reader.consumer.get_watermark_offsets.side_effect = error
        assert reader._get_beginning_offset() == OFFSET_UNINITIALIZED

    def test_unexpected_exception_reports_to_stats(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        reader.consumer = Mock(spec=KafkaConsumer)
        reader.consumer.get_watermark_offsets.side_effect = RuntimeError("boom")
        assert reader._get_beginning_offset() == OFFSET_UNINITIALIZED
        reader.stats.unexpected_exception.assert_called_once()


class TestIsReadyErrorHandling:
    @pytest.mark.parametrize(
        "error",
        [KafkaTimeoutError(), UnknownTopicOrPartitionError(), LeaderNotAvailableError(), NotLeaderForPartitionError()],
    )
    def test_known_errors_return_false(self, karapace_container: KarapaceContainer, error: Exception) -> None:
        reader = _make_schema_reader(karapace_container)
        reader.consumer = Mock(spec=KafkaConsumer)
        reader.consumer.get_watermark_offsets.side_effect = error
        assert reader._is_ready() is False

    def test_unexpected_exception_returns_false_and_reports_stats(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        reader.consumer = Mock(spec=KafkaConsumer)
        reader.consumer.get_watermark_offsets.side_effect = RuntimeError("boom")
        assert reader._is_ready() is False
        reader.stats.unexpected_exception.assert_called_once()

    def test_position_lookup_exception_is_swallowed(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        reader.consumer = Mock(spec=KafkaConsumer)
        reader.consumer.get_watermark_offsets.return_value = (0, 10)
        reader.consumer.position.side_effect = RuntimeError("boom")
        reader.offset = 2

        assert reader._is_ready(consumed_batch_empty=True) is False
        reader.stats.unexpected_exception.assert_called_once()

    def test_replay_completed_logged_only_once(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        reader.consumer = Mock(spec=KafkaConsumer)
        reader.consumer.get_watermark_offsets.return_value = (0, 1)
        reader.offset = 1

        assert reader._is_ready() is True
        assert reader._replay_completed_logged is True
        # Second call takes the "already logged" branch instead of re-logging completion.
        assert reader._is_ready() is True


class TestHandleMessagesMasterCoordinator:
    def test_watch_offsets_true_when_primary(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container, master_coordinator=Mock())
        reader.consumer = Mock(spec=KafkaConsumer)
        reader.consumer.consume.return_value = []
        reader.consumer.get_watermark_offsets.return_value = (0, 0)
        reader.master_coordinator.get_master_info.return_value = PrimaryInfo(primary=True, primary_url=None)

        with patch.object(reader, "consume_messages") as mock_consume:
            reader.handle_messages()

        mock_consume.assert_called_once_with([], True)

    def test_watch_offsets_false_when_not_primary(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container, master_coordinator=Mock())
        reader.consumer = Mock(spec=KafkaConsumer)
        reader.consumer.consume.return_value = []
        reader.consumer.get_watermark_offsets.return_value = (0, 0)
        reader.master_coordinator.get_master_info.return_value = PrimaryInfo(primary=False, primary_url=None)

        with patch.object(reader, "consume_messages") as mock_consume:
            reader.handle_messages()

        mock_consume.assert_called_once_with([], False)


class TestConsumeMessagesErrorBranches:
    def test_message_with_kafka_error_is_translated_and_raised(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        message = Mock(spec=Message)
        message.error.return_value = Mock()
        message.offset.return_value = 1

        with (
            patch(
                "karapace.core.schema_reader.translate_from_kafkaerror", return_value=RuntimeError("translated")
            ) as mock_translate,
            pytest.raises(RuntimeError, match="translated"),
        ):
            reader.consume_messages([message], False)

        mock_translate.assert_called_once()

    def test_empty_key_with_non_empty_value_continues_in_non_strict_mode(
        self, karapace_container: KarapaceContainer
    ) -> None:
        reader = _make_schema_reader(karapace_container)
        message = Mock(spec=Message)
        message.key.return_value = None
        message.value.return_value = b'{"some": "value"}'
        message.error.return_value = None
        message.offset.return_value = 3

        reader.consume_messages([message], False)  # must not raise

        assert reader.offset == 3

    def test_group_authorization_error_continues_in_non_strict_mode(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        message = Mock(spec=Message)
        message.key.return_value = b'{"keytype":"SCHEMA"}'
        message.value.return_value = b"{}"
        message.error.return_value = Mock()
        message.offset.return_value = 2

        with patch("karapace.core.schema_reader.translate_from_kafkaerror", return_value=GroupAuthorizationFailedError()):
            reader.consume_messages([message], False)  # must not raise

    def test_topic_authorization_error_raises_shutdown_in_strict_mode(self, karapace_container: KarapaceContainer) -> None:
        config = karapace_container.config().set_config_defaults({"kafka_schema_reader_strict_mode": True})
        reader = _make_schema_reader(karapace_container, config=config)
        message = Mock(spec=Message)
        message.key.return_value = b'{"keytype":"SCHEMA"}'
        message.value.return_value = b"{}"
        message.error.return_value = Mock()
        message.offset.return_value = 2

        with (
            patch("karapace.core.schema_reader.translate_from_kafkaerror", return_value=TopicAuthorizationFailedError()),
            pytest.raises(ShutdownException),
        ):
            reader.consume_messages([message], False)

    def test_deprecated_key_format_switches_keymode_and_counts_are_tracked(
        self, karapace_container: KarapaceContainer
    ) -> None:
        reader = _make_schema_reader(karapace_container, key_formatter=KeyFormatter())
        assert reader.key_formatter.get_keymode() == KeyMode.CANONICAL
        message = Mock(spec=Message)
        # Field order ("magic" before "keytype") makes this key non-canonical, and a
        # NOOP keytype exercises `handle_msg`'s no-op dispatch branch with no side effects.
        message.key.return_value = b'{"magic":0,"keytype":"NOOP"}'
        message.value.return_value = None
        message.error.return_value = None
        message.offset.return_value = 4

        reader.consume_messages([message], False)

        assert reader.key_formatter.get_keymode() == KeyMode.DEPRECATED_KARAPACE
        assert reader.offset == 4

    def test_offset_seen_recorded_when_ready_and_watching(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container, key_formatter=KeyFormatter())
        reader._ready = True
        message = Mock(spec=Message)
        message.key.return_value = b'{"keytype":"NOOP","magic":0}'
        message.value.return_value = None
        message.error.return_value = None
        message.offset.return_value = 9

        reader.consume_messages([message], True)

        assert reader._offset_watcher.greatest_offset() == 9


class TestUpdateIsReadyFlag:
    def test_calls_is_ready_when_not_ready(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        reader._ready = False

        with patch.object(reader, "_is_ready", return_value=True) as mock_is_ready:
            reader._update_is_ready_flag(consumed_batch_empty=True)

        mock_is_ready.assert_called_once_with(consumed_batch_empty=True)
        assert reader.ready() is True

    def test_skips_is_ready_when_already_ready(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        reader._ready = True

        with patch.object(reader, "_is_ready") as mock_is_ready:
            reader._update_is_ready_flag()

        mock_is_ready.assert_not_called()


class TestHandleMsgConfig:
    def test_creates_subject_and_sets_compatibility(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        reader._handle_msg_config({"subject": "s"}, {"compatibilityLevel": "FULL"})
        assert reader.database.find_subject(subject=Subject("s")) == Subject("s")
        assert reader.database.get_subject_compatibility(subject=Subject("s")) == "FULL"

    def test_deletes_compatibility_when_value_falsy(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        reader.database.insert_subject(subject=Subject("s"))
        reader.database.set_subject_compatibility(subject=Subject("s"), compatibility="FULL")

        reader._handle_msg_config({"subject": "s"}, None)

        assert reader.database.get_subject_compatibility(subject=Subject("s")) is None

    def test_sets_global_compatibility_when_no_subject(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        reader._handle_msg_config({"subject": None}, {"compatibilityLevel": "BACKWARD"})
        assert reader.config.compatibility == "BACKWARD"


class TestHandleMsgDeleteSubject:
    def test_raises_value_error_when_value_missing(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        with pytest.raises(ValueError):
            reader._handle_msg_delete_subject({}, None)

    def test_warns_when_subject_unknown(self, karapace_container: KarapaceContainer, caplog: LogCaptureFixture) -> None:
        reader = _make_schema_reader(karapace_container)
        with caplog.at_level(logging.WARNING, logger="karapace.core.schema_reader"):
            reader._handle_msg_delete_subject({}, {"subject": "unknown", "version": 1})
        assert any("did not exist" in r.message for r in caplog.records)

    def test_deletes_existing_subject_versions(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        subject = Subject("s")
        reader.database.insert_subject(subject=subject)
        schema = _avro_schema()
        schema_id = reader.database.get_schema_id(schema)
        reader.database.insert_schema_version(
            subject=subject, schema_id=schema_id, version=Version(1), schema=schema, deleted=False, references=None
        )

        reader._handle_msg_delete_subject({}, {"subject": "s", "version": 1})

        assert reader.database.find_subject_schemas(subject=subject, include_deleted=True)[Version(1)].deleted is True


class TestHandleMsgSchemaHardDelete:
    def test_warns_when_subject_unknown(self, karapace_container: KarapaceContainer, caplog: LogCaptureFixture) -> None:
        reader = _make_schema_reader(karapace_container)
        with caplog.at_level(logging.WARNING, logger="karapace.core.schema_reader"):
            reader._handle_msg_schema_hard_delete({"subject": "unknown", "version": 1})
        assert any("did not exist" in r.message for r in caplog.records)

    def test_deletes_last_version_and_removes_subject(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        subject = Subject("s")
        reader.database.insert_subject(subject=subject)
        schema = _avro_schema()
        schema_id = reader.database.get_schema_id(schema)
        reader.database.insert_schema_version(
            subject=subject, schema_id=schema_id, version=Version(1), schema=schema, deleted=False, references=None
        )

        reader._handle_msg_schema_hard_delete({"subject": "s", "version": 1})

        assert reader.database.find_subject(subject=subject) is None

    def test_deletes_one_of_several_versions_keeps_subject(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        subject = Subject("s")
        reader.database.insert_subject(subject=subject)
        for v, name in ((1, "First"), (2, "Second")):
            schema = _avro_schema(name)
            schema_id = reader.database.get_schema_id(schema)
            reader.database.insert_schema_version(
                subject=subject, schema_id=schema_id, version=Version(v), schema=schema, deleted=False, references=None
            )

        reader._handle_msg_schema_hard_delete({"subject": "s", "version": 1})

        assert reader.database.find_subject(subject=subject) is not None
        assert Version(1) not in reader.database.find_subject_schemas(subject=subject, include_deleted=True)


class TestHandleMsgSchema:
    def test_falsy_value_dispatches_to_hard_delete(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        with patch.object(reader, "_handle_msg_schema_hard_delete") as mock_hard_delete:
            reader._handle_msg_schema({"subject": "s", "version": 1}, None)
        mock_hard_delete.assert_called_once_with({"subject": "s", "version": 1})

    def test_invalid_schema_type_raises_invalid_schema(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        with pytest.raises(InvalidSchema):
            reader._handle_msg_schema({}, {"schemaType": "BOGUS", "schema": "{}", "subject": "s", "id": 1, "version": 1})

    def test_jsonschema_invalid_json_raises_invalid_schema(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        with pytest.raises(InvalidSchema):
            reader._handle_msg_schema(
                {}, {"schemaType": "JSON", "schema": "not-json", "subject": "s", "id": 1, "version": 1}
            )

    def test_protobuf_schema_is_parsed_and_stored(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        schema_str = "syntax = 'proto3'; message Test { string test = 1; }"

        reader._handle_msg_schema(
            {}, {"schemaType": "PROTOBUF", "schema": schema_str, "subject": "s", "id": 1, "version": 1}
        )

        stored = reader.database.find_schema(schema_id=SchemaId(1))
        assert stored is not None
        assert stored.schema_type == SchemaType.PROTOBUF

    def test_typed_schema_construction_failure_raises_invalid_schema(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        with (
            patch("karapace.core.schema_reader.TypedSchema", side_effect=InvalidSchema("bad")),
            pytest.raises(InvalidSchema),
        ):
            reader._handle_msg_schema({}, {"schemaType": "AVRO", "schema": "{}", "subject": "s", "id": 1, "version": 1})


class TestHandleMsgNoOperation:
    def test_no_operation_keytype_is_a_noop(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        reader.handle_msg({"keytype": "NOOP"}, None)  # must not raise


class TestGetReferencedBy:
    def test_delegates_to_database(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        referenced_subject = Subject("ref_subject")
        reader.database.insert_subject(subject=referenced_subject)
        ref_schema = _avro_schema("Ref")
        ref_schema_id = reader.database.get_schema_id(ref_schema)
        reader.database.insert_schema_version(
            subject=referenced_subject,
            schema_id=ref_schema_id,
            version=Version(1),
            schema=ref_schema,
            deleted=False,
            references=None,
        )

        consumer_subject = Subject("consumer")
        reader.database.insert_subject(subject=consumer_subject)
        consumer_schema = _avro_schema("Consumer")
        consumer_schema_id = reader.database.get_schema_id(consumer_schema)
        reference = Reference(name="r", subject=referenced_subject, version=Version(1))
        reader.database.insert_schema_version(
            subject=consumer_subject,
            schema_id=consumer_schema_id,
            version=Version(1),
            schema=consumer_schema,
            deleted=False,
            references=[reference],
        )

        referents = reader.get_referenced_by(referenced_subject, Version(1))
        assert referents == {consumer_schema_id}


class TestResolveAndValidate:
    def test_validates_schema_without_references(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        schema = TypedSchema(
            schema_type=SchemaType.AVRO, schema_str=json.dumps({"type": "record", "name": "Obj", "fields": []})
        )
        result = reader._resolve_and_validate(schema)
        assert isinstance(result, TypedSchema)
        assert result.schema_type == SchemaType.AVRO


class TestResolveReference:
    def _insert_referenced_schema(
        self, reader: KafkaSchemaReader, subject_name: str = "ref_subject", version: int = 1
    ) -> Subject:
        subject = Subject(subject_name)
        reader.database.insert_subject(subject=subject)
        schema = ValidatedTypedSchema.parse(
            schema_type=SchemaType.AVRO, schema_str=json.dumps({"type": "record", "name": "Ref", "fields": []})
        )
        schema_id = reader.database.get_schema_id(schema)
        reader.database.insert_schema_version(
            subject=subject, schema_id=schema_id, version=Version(version), schema=schema, deleted=False, references=None
        )
        return subject

    def test_raises_when_subject_not_found(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        with pytest.raises(InvalidReferences, match="Subject not found"):
            reader._resolve_reference(Reference(name="r", subject=Subject("missing"), version=Version(1)))

    def test_resolves_latest_version_reference(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        subject = self._insert_referenced_schema(reader)

        resolved_ref, dependency = reader._resolve_reference(LatestVersionReference(name="r", subject=subject))

        assert resolved_ref.version == Version(1)
        assert dependency.name == "r"

    def test_raises_when_version_not_found(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        subject = self._insert_referenced_schema(reader)
        with pytest.raises(InvalidReferences, match="no such schema version"):
            reader._resolve_reference(Reference(name="r", subject=subject, version=Version(99)))

    def test_raises_when_schema_version_has_no_schema(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        subject = Subject("s")
        reader.database.insert_subject(subject=subject)

        # Force a `SchemaVersion` whose `.schema` is falsy to hit the defensive guard.
        with (
            patch.object(InMemoryDatabase, "find_subject_schemas", return_value={Version(1): Mock(schema=None)}),
            pytest.raises(InvalidReferences, match="No schema in"),
        ):
            reader._resolve_reference(Reference(name="r", subject=subject, version=Version(1)))


class TestResolveReferences:
    def test_accepts_mapping_references_and_resolves_them(self, karapace_container: KarapaceContainer) -> None:
        reader = _make_schema_reader(karapace_container)
        subject = Subject("ref_subject")
        reader.database.insert_subject(subject=subject)
        schema = ValidatedTypedSchema.parse(
            schema_type=SchemaType.AVRO, schema_str=json.dumps({"type": "record", "name": "Ref", "fields": []})
        )
        schema_id = reader.database.get_schema_id(schema)
        reader.database.insert_schema_version(
            subject=subject, schema_id=schema_id, version=Version(1), schema=schema, deleted=False, references=None
        )

        resolved_references, dependencies = reader.resolve_references(
            [{"name": "r", "subject": "ref_subject", "version": 1}]
        )

        assert resolved_references[0].name == "r"
        assert "r" in dependencies
