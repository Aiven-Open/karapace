"""
karapace - Key correction unit tests

Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from aiokafka.structs import TopicPartition
from dataclasses import dataclass
from karapace import key_format_corrector
from typing import Any, Dict, Optional, Union
from unittest.mock import call, MagicMock, Mock, patch

import asyncio
import collections
import json
import pytest


def _key_bytes(key: Dict[str, Union[str, int]]) -> bytes:
    return json.dumps(key, sort_keys=False, separators=(",", ":")).encode("utf-8")


@dataclass
class CheckAndStoreTestCase:
    name: str
    original_key: Optional[bytes]
    corrected_key: Optional[bytes]
    expected_futures_count: int


@pytest.mark.parametrize(
    "test_case",
    [
        CheckAndStoreTestCase(
            name="Happy case of corrected schema record key.",
            original_key=_key_bytes(
                {
                    "magic": 1,
                    "subject": "test-subject",
                    "version": 1,
                    "keytype": "SCHEMA",
                }
            ),
            corrected_key=_key_bytes(
                {
                    "keytype": "SCHEMA",
                    "subject": "test-subject",
                    "version": 1,
                    "magic": 1,
                }
            ),
            expected_futures_count=2,
        ),
        CheckAndStoreTestCase(
            name="Happy case of corrected delete subject record key.",
            original_key=_key_bytes(
                {
                    "magic": 0,
                    "subject": "test-subject",
                    "keytype": "DELETE_SUBJECT",
                }
            ),
            corrected_key=_key_bytes(
                {
                    "keytype": "DELETE_SUBJECT",
                    "subject": "test-subject",
                    "magic": 0,
                }
            ),
            expected_futures_count=2,
        ),
        CheckAndStoreTestCase(
            name="Happy case of corrected subject config record key.",
            original_key=_key_bytes(
                {
                    "magic": 0,
                    "subject": "test-subject",
                    "keytype": "CONFIG",
                }
            ),
            corrected_key=_key_bytes(
                {
                    "keytype": "CONFIG",
                    "subject": "test-subject",
                    "magic": 0,
                }
            ),
            expected_futures_count=2,
        ),
        CheckAndStoreTestCase(
            name="Happy case of corrected config record key.",
            original_key=_key_bytes(
                {
                    "magic": 0,
                    "subject": None,
                    "keytype": "CONFIG",
                }
            ),
            corrected_key=_key_bytes(
                {
                    "keytype": "CONFIG",
                    "subject": None,
                    "magic": 0,
                }
            ),
            expected_futures_count=2,
        ),
        CheckAndStoreTestCase(
            name="Happy case of existing correct key.",
            original_key=_key_bytes(
                {
                    "keytype": "SCHEMA",
                    "subject": "test-subject",
                    "version": 1,
                    "magic": 1,
                }
            ),
            corrected_key=_key_bytes(
                {
                    "keytype": "SCHEMA",
                    "subject": "test-subject",
                    "version": 1,
                    "magic": 1,
                }
            ),
            expected_futures_count=2,
        ),
        CheckAndStoreTestCase(
            name="Happy case of correction with unknown keytype.",
            original_key=_key_bytes(
                {
                    "magic": 1,
                    "version": 1,
                    "keytype": "UNKNOWN_KEY_TYPE",
                    "subject": "test-subject",
                }
            ),
            corrected_key=_key_bytes(
                {
                    "keytype": "UNKNOWN_KEY_TYPE",
                    "subject": "test-subject",
                    "version": 1,
                    "magic": 1,
                }
            ),
            expected_futures_count=2,
        ),
        CheckAndStoreTestCase(
            name="Invalid record, missing mandatory keytype property.",
            original_key=_key_bytes(
                {
                    "magic": 1,
                    "subject": "test-subject",
                    "version": 1,
                }
            ),
            corrected_key=None,
            expected_futures_count=0,
        ),
        CheckAndStoreTestCase(
            name="Invalid record, missing mandatory magic property.",
            original_key=_key_bytes(
                {
                    "subject": "test-subject",
                    "keytype": "SCHEMA",
                    "version": 1,
                }
            ),
            corrected_key=None,
            expected_futures_count=0,
        ),
        CheckAndStoreTestCase(
            name="Invalid record, key is none",
            original_key=None,
            corrected_key=None,
            expected_futures_count=0,
        ),
        CheckAndStoreTestCase(
            name="Invalid JSON as key.",
            original_key=b'{"invalid":"json"}',
            corrected_key=None,
            expected_futures_count=0,
        ),
    ],
)
async def test_check_and_store(test_case: CheckAndStoreTestCase) -> None:
    config = {
        "topic_name": "_schemas",
    }
    kfc = key_format_corrector.KeyFormatCorrector(config=config)

    producer_mock = Mock()
    producer_mock.send = Mock()
    returned_future = asyncio.Future()
    producer_mock.send.return_value = returned_future
    test_value = b"Test value"

    if test_case.expected_futures_count > 0:
        returned_future.set_result(True)
    else:
        returned_future.set_result(None)

    futures = await kfc.check_and_store(producer=producer_mock, original_key=test_case.original_key, value=test_value)
    if test_case.expected_futures_count == 2:
        assert futures is not None
        assert len(futures) == test_case.expected_futures_count
        producer_mock.send.assert_has_calls(
            (
                call("_schemas", key=test_case.original_key, value=None),
                call("_schemas", key=test_case.corrected_key, value=test_value),
            )
        )
    else:
        assert futures is None
        producer_mock.send.assert_not_called()


def mock_async_response(result: Any = None) -> asyncio.Future:
    """Python 3.7 is supported and no AsyncMock available. Use this for mocking async responses."""
    future = asyncio.Future()
    if result is None:
        result = asyncio.Future()
        result.set_result(True)
    future.set_result(result)
    return future


async def test_run_correction_invalid_schemas_topic_partitioning() -> None:
    with patch.object(key_format_corrector, "AIOKafkaProducer", return_value=None) as mock_producer, patch.object(
        key_format_corrector, "AIOKafkaConsumer", return_value=None
    ) as mock_consumer:
        mock_producer_instance = MagicMock()
        mock_consumer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_consumer.return_value = mock_consumer_instance

        mock_consumer_instance.start.return_value = mock_async_response()
        mock_producer_instance.start.return_value = mock_async_response()
        mock_producer_instance.flush.return_value = mock_async_response()
        mock_consumer_instance.stop.return_value = mock_async_response()
        mock_producer_instance.stop.return_value = mock_async_response()

        mock_consumer_instance.partitions_for_topic = Mock()
        mock_consumer_instance.partitions_for_topic.return_value = set([1, 2])

        config = {
            "topic_name": "_schemas",
            "bootstrap_uri": "localhost:1234",
        }
        kfc = key_format_corrector.KeyFormatCorrector(config=config)
        await kfc.run_correction()

        # Start called
        mock_producer_instance.start.assert_called_once()
        mock_consumer_instance.start.assert_called_once()

        # Invalid partitioning, nothing produced
        mock_consumer_instance.getmany.assert_not_called()
        mock_producer_instance.send.assert_not_called()
        mock_producer_instance.send_and_wait.assert_not_called()

        # Finally block
        assert not kfc.running
        mock_producer_instance.flush.assert_called_once()
        mock_producer_instance.stop.assert_called_once()
        mock_consumer_instance.stop.assert_called_once()

        # Test case of failed key correction and re-try of running the correction
        mock_producer.reset_mock()
        mock_consumer.reset_mock()
        await kfc.run_correction()
        mock_producer.assert_not_called()
        mock_consumer.assert_not_called()


async def test_run_correction_latest_offset_is_end_offset() -> None:
    with patch.object(key_format_corrector, "AIOKafkaProducer", return_value=None) as mock_producer, patch.object(
        key_format_corrector, "AIOKafkaConsumer", return_value=None
    ) as mock_consumer:
        mock_producer_instance = MagicMock()
        mock_consumer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_consumer.return_value = mock_consumer_instance

        mock_consumer_instance.start.return_value = mock_async_response()
        mock_producer_instance.start.return_value = mock_async_response()
        mock_producer_instance.flush.return_value = mock_async_response()
        mock_consumer_instance.stop.return_value = mock_async_response()
        mock_producer_instance.stop.return_value = mock_async_response()

        mock_consumer_instance.partitions_for_topic = Mock()
        mock_consumer_instance.partitions_for_topic.return_value = set([0])
        topic_partition = TopicPartition("_schemas", 0)
        mock_consumer_instance.end_offsets.return_value = mock_async_response({topic_partition: 0})
        mock_producer_instance.send_and_wait.return_value = mock_async_response()

        config = {
            "topic_name": "_schemas",
            "bootstrap_uri": "localhost:1234",
        }
        kfc = key_format_corrector.KeyFormatCorrector(config=config)
        await kfc.run_correction()
        assert kfc.key_correction_done

        # Start called
        mock_producer_instance.start.assert_called_once()
        mock_consumer_instance.start.assert_called_once()

        # Offset already satisfied, nothing produced
        mock_consumer_instance.getmany.assert_not_called()
        mock_producer_instance.send.assert_not_called()
        mock_producer_instance.send_and_wait.assert_called_once()

        # Finally block
        assert not kfc.running
        mock_producer_instance.flush.assert_called_once()
        mock_producer_instance.stop.assert_called_once()
        mock_consumer_instance.stop.assert_called_once()


async def test_run_correction_no_messages() -> None:
    with patch.object(key_format_corrector, "AIOKafkaProducer", return_value=None) as mock_producer, patch.object(
        key_format_corrector, "AIOKafkaConsumer", return_value=None
    ) as mock_consumer:
        mock_producer_instance = MagicMock()
        mock_consumer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_consumer.return_value = mock_consumer_instance

        mock_consumer_instance.start.return_value = mock_async_response()
        mock_producer_instance.start.return_value = mock_async_response()
        mock_producer_instance.flush.return_value = mock_async_response()
        mock_consumer_instance.stop.return_value = mock_async_response()
        mock_producer_instance.stop.return_value = mock_async_response()

        mock_consumer_instance.partitions_for_topic = Mock()
        mock_consumer_instance.partitions_for_topic.return_value = set([0])
        topic_partition = TopicPartition("_schemas", 0)
        mock_consumer_instance.end_offsets.return_value = mock_async_response({topic_partition: 10})
        mock_producer_instance.send_and_wait.return_value = mock_async_response()
        mock_consumer_instance.getmany.return_value = mock_async_response({})

        config = {
            "topic_name": "_schemas",
            "bootstrap_uri": "localhost:1234",
        }
        kfc = key_format_corrector.KeyFormatCorrector(config=config)
        kfc.running = True
        await kfc.run_correction()
        assert kfc.key_correction_done

        # Start called
        mock_producer_instance.start.assert_called_once()
        mock_consumer_instance.start.assert_called_once()

        # No messages, nothing produced
        mock_consumer_instance.getmany.assert_called_once()
        mock_producer_instance.send.assert_not_called()
        mock_producer_instance.send_and_wait.assert_called_once()

        # Finally block
        assert not kfc.running
        mock_producer_instance.flush.assert_called_once()
        mock_producer_instance.stop.assert_called_once()
        mock_consumer_instance.stop.assert_called_once()


async def test_run_correction_getmany_fails() -> None:
    with patch.object(key_format_corrector, "AIOKafkaProducer", return_value=None) as mock_producer, patch.object(
        key_format_corrector, "AIOKafkaConsumer", return_value=None
    ) as mock_consumer:
        mock_producer_instance = MagicMock()
        mock_consumer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_consumer.return_value = mock_consumer_instance

        mock_consumer_instance.start.return_value = mock_async_response()
        mock_producer_instance.start.return_value = mock_async_response()
        mock_producer_instance.flush.return_value = mock_async_response()
        mock_consumer_instance.stop.return_value = mock_async_response()
        mock_producer_instance.stop.return_value = mock_async_response()

        mock_consumer_instance.partitions_for_topic = Mock()
        mock_consumer_instance.partitions_for_topic.return_value = set([0])
        topic_partition = TopicPartition("_schemas", 0)
        mock_consumer_instance.end_offsets.return_value = mock_async_response({topic_partition: 10})
        mock_producer_instance.send_and_wait.return_value = mock_async_response()
        mock_consumer_instance.getmany.side_effect = Exception("Except")

        config = {
            "topic_name": "_schemas",
            "bootstrap_uri": "localhost:1234",
        }
        kfc = key_format_corrector.KeyFormatCorrector(config=config)
        kfc.running = True

        with pytest.raises(Exception, match="Except"):
            await kfc.run_correction()
        assert not kfc.key_correction_done

        # Start called
        mock_producer_instance.start.assert_called_once()
        mock_consumer_instance.start.assert_called_once()

        mock_consumer_instance.getmany.assert_called_once()
        mock_producer_instance.send.assert_not_called()
        mock_producer_instance.send_and_wait.assert_not_called()

        # Finally block
        assert not kfc.running
        mock_producer_instance.flush.assert_called_once()
        mock_producer_instance.stop.assert_called_once()
        mock_consumer_instance.stop.assert_called_once()

        # Test case of failed key correction and re-try of running the correction
        mock_producer.reset_mock()
        mock_consumer.reset_mock()
        await kfc.run_correction()
        mock_producer.assert_not_called()
        mock_consumer.assert_not_called()


async def test_run_correction_msg_offset_after() -> None:
    with patch.object(key_format_corrector, "AIOKafkaProducer", return_value=None) as mock_producer, patch.object(
        key_format_corrector, "AIOKafkaConsumer", return_value=None
    ) as mock_consumer:
        mock_producer_instance = MagicMock()
        mock_consumer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_consumer.return_value = mock_consumer_instance

        mock_consumer_instance.start.return_value = mock_async_response()
        mock_producer_instance.start.return_value = mock_async_response()
        mock_producer_instance.flush.return_value = mock_async_response()
        mock_consumer_instance.stop.return_value = mock_async_response()
        mock_producer_instance.stop.return_value = mock_async_response()

        mock_consumer_instance.partitions_for_topic = Mock()
        mock_consumer_instance.partitions_for_topic.return_value = set([0])
        topic_partition = TopicPartition("_schemas", 0)
        mock_consumer_instance.end_offsets.return_value = mock_async_response({topic_partition: 1})
        mock_producer_instance.send_and_wait.return_value = mock_async_response()

        ConsumerRecord = collections.namedtuple("ConsumerRecord", ["offset", "key", "value"])
        invalid_key = json.dumps(
            {
                "magic": 1,
                "subject": "test-subject",
                "version": 1,
                "keytype": "SCHEMA",
            },
            sort_keys=False,
            separators=(",", ":"),
        ).encode("utf-8")

        mock_consumer_instance.getmany.return_value = mock_async_response(
            {topic_partition: [ConsumerRecord(offset=2, key=invalid_key, value=b"v_one")]}
        )

        config = {
            "topic_name": "_schemas",
            "bootstrap_uri": "localhost:1234",
        }
        kfc = key_format_corrector.KeyFormatCorrector(config=config)
        kfc.running = True
        await kfc.run_correction()
        assert kfc.key_correction_done

        # Start called
        mock_producer_instance.start.assert_called_once()
        mock_consumer_instance.start.assert_called_once()

        # Nothing sent as the sole message offset is after
        mock_consumer_instance.getmany.assert_called_once()
        mock_producer_instance.send.assert_not_called()
        mock_producer_instance.send_and_wait.assert_called_once()

        # Finally block
        assert not kfc.running
        mock_producer_instance.flush.assert_called_once()
        mock_producer_instance.stop.assert_called_once()
        mock_consumer_instance.stop.assert_called_once()


async def test_run_correction() -> None:
    with patch.object(key_format_corrector, "AIOKafkaProducer", return_value=None) as mock_producer, patch.object(
        key_format_corrector, "AIOKafkaConsumer", return_value=None
    ) as mock_consumer:
        mock_producer_instance = MagicMock()
        mock_consumer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_consumer.return_value = mock_consumer_instance

        mock_consumer_instance.start.return_value = mock_async_response()
        mock_producer_instance.start.return_value = mock_async_response()
        mock_producer_instance.flush.return_value = mock_async_response()
        mock_consumer_instance.stop.return_value = mock_async_response()
        mock_producer_instance.stop.return_value = mock_async_response()

        mock_consumer_instance.partitions_for_topic = Mock()
        mock_consumer_instance.partitions_for_topic.return_value = set([0])
        topic_partition = TopicPartition("_schemas", 0)
        mock_consumer_instance.end_offsets.return_value = mock_async_response({topic_partition: 1})
        mock_producer_instance.send.return_value = mock_async_response()
        mock_producer_instance.send_and_wait.return_value = mock_async_response()

        ConsumerRecord = collections.namedtuple("ConsumerRecord", ["offset", "key", "value"])
        invalid_key = json.dumps(
            {
                "magic": 1,
                "subject": "test-subject",
                "version": 1,
                "keytype": "SCHEMA",
            },
            sort_keys=False,
            separators=(",", ":"),
        ).encode("utf-8")
        corrected_key = json.dumps(
            {
                "keytype": "SCHEMA",
                "subject": "test-subject",
                "version": 1,
                "magic": 1,
            },
            sort_keys=False,
            separators=(",", ":"),
        ).encode("utf-8")
        good_key = json.dumps(
            {
                "keytype": "SCHEMA",
                "subject": "test-subject",
                "version": 2,
                "magic": 1,
            },
            sort_keys=False,
            separators=(",", ":"),
        ).encode("utf-8")

        mock_consumer_instance.getmany.return_value = mock_async_response(
            {
                topic_partition: [
                    ConsumerRecord(offset=0, key=invalid_key, value=b"v_one"),
                    ConsumerRecord(offset=1, key=good_key, value=b"v_two"),
                ]
            }
        )

        config = {
            "topic_name": "_schemas",
            "bootstrap_uri": "localhost:1234",
        }
        kfc = key_format_corrector.KeyFormatCorrector(config=config)
        kfc.running = True
        await kfc.run_correction()
        assert kfc.key_correction_done

        # Start called
        mock_producer_instance.start.assert_called_once()
        mock_consumer_instance.start.assert_called_once()

        # Producer sends one corrected and tombstone
        mock_consumer_instance.getmany.assert_called_once()

        mock_producer_instance.send.assert_has_calls(
            (call("_schemas", key=invalid_key, value=None), call("_schemas", key=corrected_key, value=b"v_one"))
        )

        mock_producer_instance.send_and_wait.assert_called_once()

        # Finally block
        assert not kfc.running
        mock_producer_instance.flush.assert_called_once()
        mock_producer_instance.stop.assert_called_once()
        mock_consumer_instance.stop.assert_called_once()
