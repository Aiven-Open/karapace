"""
karapace - Key correction

Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition
from asyncio import Future
from karapace.config import Config
from karapace.typing import JsonData
from karapace.utils import json_encode
from typing import List, Optional

import asyncio
import json
import logging
import threading
import time

LOG = logging.getLogger(__name__)

SCHEMA_KEY_ORDER = ["keytype", "subject", "version", "magic"]
CONFIG_KEY_ORDER = ["keytype", "subject", "magic"]
NOOP_KEY_ORDER = ["keytype", "magic"]

CORRECT_KEY_ORDERS = [SCHEMA_KEY_ORDER, CONFIG_KEY_ORDER, NOOP_KEY_ORDER]


def _get_key_in_correct_order(key: JsonData) -> bytes:
    corrected_key = {
        "keytype": key["keytype"],
    }
    if "subject" in key:
        corrected_key["subject"] = key["subject"]
    if "version" in key:
        corrected_key["version"] = key["version"]
    # Magic is the last element
    corrected_key["magic"] = key["magic"]
    return json_encode(corrected_key, sort_keys=False, binary=True, compact=True)


class KeyFormatCorrector:
    """Correct message key format to use "keytype, subject, version, magic" order of keys.

    Karapace has used "subject, version, magic, keytype". This can cause issues when migrating from
    Confluent Schema Registry to Karapace. Kafka log compaction compares the key as bytes and using different
    order of keys in the contained JSON does not produce same byte output.
    """

    def __init__(self, config: Config) -> None:
        self.config = config

        self.timeout_ms = 1000
        self.running = False
        self.key_correction_done = False
        self.failed = False

        self.total_msgs = 0
        self.corrected_msgs = 0

    def stop(self) -> None:
        LOG.info("[KeyCorrector] Stopping key correction.")
        self.running = False

    def start(self) -> None:
        if not self.running and not self.key_correction_done:
            LOG.info("[KeyCorrector] Starting key correction.")
            thread = threading.Thread(target=self._run, daemon=True)
            self.running = True
            thread.start()

    def _run(self) -> None:
        asyncio.run(self.run_correction())

    async def run_correction(self) -> None:

        if self.failed:
            LOG.warning("[KeyCorrector] Correction marked failed, not attempting again.")
            return

        latest_offset = 0
        start_time = time.monotonic()
        producer = AIOKafkaProducer(
            bootstrap_servers=self.config["bootstrap_uri"],
            linger_ms=50,
        )
        consumer = AIOKafkaConsumer(
            self.config["topic_name"],
            bootstrap_servers=self.config["bootstrap_uri"],
            group_id="key-corrector",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=5000,
        )

        try:
            await producer.start()
            await consumer.start()

            # Get the offset up to run
            partitions = consumer.partitions_for_topic(self.config["topic_name"])
            if len(partitions) != 1:
                # The key correction process must be stopped so it won't be tried again when configuration
                # of schemas topic is invalid. The process can be retried by restarting Karapace.
                self.failed = True
                LOG.error(
                    ("The schema topic %s has %s partitions, expected one partition. Aborting key correction."),
                    self.config["topic_name"],
                    len(partitions),
                )
                return

            # The partitions is a set, cannot access with index.
            for partition in partitions:
                break
            tp = TopicPartition(self.config["topic_name"], partition)  # pylint: disable=undefined-loop-variable

            end_offsets = await consumer.end_offsets([tp])
            end_offset = end_offsets[tp]
            LOG.info("[KeyCorrector] Start to consume up to offset %s.", end_offset)
            if latest_offset == end_offset:
                # On empty topic the latest offset and end offset are the same.
                self.key_correction_done = True

            while self.running and latest_offset < end_offset:
                raw_msgs = await consumer.getmany(timeout_ms=self.timeout_ms)
                # All done if no more messages
                if not raw_msgs:
                    LOG.info("[KeyCorrector] No more messages, done for today.")
                    # All done if no more messages
                    self.key_correction_done = True
                    break

                for _, messages in raw_msgs.items():
                    futures = []
                    for message in messages:
                        self.total_msgs += 1
                        latest_offset = max(message.offset, latest_offset)
                        if message.offset < end_offset:
                            maybe_futures = await self.check_and_store(producer, message.key, message.value)
                            if maybe_futures:
                                futures += maybe_futures
                        else:
                            LOG.debug("[KeyCorrector] Message after end_offset.")
                            break
                    if futures:
                        await asyncio.wait(futures, timeout=30000)
                    LOG.info(
                        "Processed %s messages, messages/s %s",
                        self.total_msgs,
                        (self.total_msgs / (time.monotonic() - start_time)),
                    )
                if latest_offset >= end_offset:
                    LOG.info("[KeyCorrector] latest_offset greater or equal to end_offset, done for today.")
                    self.key_correction_done = True

            LOG.info("[KeyCorrector] Key correction took %s seconds.", time.monotonic() - start_time)
            LOG.info("[KeyCorrector] Total: %s - Corrected: %s.", self.total_msgs, self.corrected_msgs)
            LOG.info("[KeyCorrector] Handled to offset of %s.", latest_offset)
        except Exception as e:
            # Log status of key correction in case of unexpected exception
            self.failed = True
            LOG.error("Key correction failed")
            LOG.error("[KeyCorrector] Key correction took %s seconds.", time.monotonic() - start_time)
            LOG.error("[KeyCorrector] Total: %s - Corrected: %s.", self.total_msgs, self.corrected_msgs)
            LOG.error("[KeyCorrector] Handled to offset of %s.", latest_offset)
            raise e
        finally:
            if self.key_correction_done:
                await self._send_key_correction_done_marker(producer)
            self.running = False
            await producer.flush()
            await producer.stop()
            await consumer.stop()

    async def check_and_store(
        self, producer: AIOKafkaProducer, original_key: Optional[bytes], value: Optional[bytes]
    ) -> Optional[List[Future]]:
        if original_key is None:
            LOG.warning("Invalid key 'None'.")
            return None
        try:
            original_key_dict = json.loads(original_key)
        except (json.JSONDecodeError) as e:
            LOG.warning("Invalid key data", exc_info=e)
            return None
        if list(original_key_dict.keys()) not in CORRECT_KEY_ORDERS:
            try:
                return await self.correct(producer, _get_key_in_correct_order(original_key_dict), original_key, value)
            except KeyError as e:
                LOG.warning("Invalid message in schema topic.", exc_info=e)
                return None
        else:
            # Send message also to retain the order in topic.
            return await self.correct(producer, original_key, original_key, value)

    async def correct(
        self, producer: AIOKafkaProducer, corrected_key: bytes, original_key: bytes, value: Optional[bytes]
    ) -> List[Future]:
        futures: List[Future] = []

        # Send delete with invalid key, compaction will remove eventually
        future = await producer.send(self.config["topic_name"], key=original_key, value=None)
        futures.append(future)

        # Send with corrected key after the tombstone as tombstone is a delete event
        future = await producer.send(self.config["topic_name"], key=corrected_key, value=value)
        futures.append(future)

        self.corrected_msgs += 1
        return futures

    async def _send_key_correction_done_marker(self, producer: AIOKafkaProducer) -> None:
        key = '{"keytype":"KEY_CORRECTION_DONE","magic":0}'.encode("utf-8")
        value = b""
        await producer.send_and_wait(self.config["topic_name"], key=key, value=value)
        LOG.info("[KeyCorrector] Key correction done message sent.")
