from asyncio import Lock
from collections import defaultdict, namedtuple
from functools import partial
from kafka import KafkaConsumer
from kafka.errors import IllegalStateError, KafkaConfigurationError, KafkaError
from kafka.structs import OffsetAndMetadata, TopicPartition
from karapace.karapace import empty_response, KarapaceBase
from karapace.serialization import InvalidMessageHeader, InvalidPayload, SchemaRegistryDeserializer
from karapace.utils import convert_to_int
from struct import error as UnpackError
from typing import Tuple
from urllib.parse import urljoin

import asyncio
import base64
import json
import logging
import time
import uuid

KNOWN_FORMATS = {"json", "avro", "binary"}
OFFSET_RESET_STRATEGIES = {"latest", "earliest"}

TypedConsumer = namedtuple("TypedConsumer", ["consumer", "serialization_format", "config"])


class ConsumerManager:
    def __init__(self, config_path: str):
        self.config = KarapaceBase.read_config(config_path)
        self.hostname = f"http://{self.config['advertised_hostname']}:{self.config['port']}"
        self.log = logging.getLogger("RestConsumerManager")
        self.deserializer = SchemaRegistryDeserializer(config_path=config_path)
        self.consumers = {}
        self.consumer_locks = defaultdict(Lock)

    def new_name(self) -> str:
        name = str(uuid.uuid4())
        self.log.debug("Generated new consumer name: %s", name)
        return name

    @staticmethod
    def _assert(cond: bool, code: int, sub_code: int, message: str, content_type: str):
        if not cond:
            KarapaceBase.r(content_type=content_type, status=code, body={"message": message, "error_code": sub_code})

    def _assert_consumer_exists(self, internal_name: Tuple[str, str], content_type: str):
        if internal_name not in self.consumers:
            KarapaceBase.not_found(
                message=f"Consumer for {internal_name} not found among {list(self.consumers.keys())}",
                content_type=content_type,
                sub_code=40403
            )

    @staticmethod
    def _assert_positive_number(container: dict, key: str, content_type: str, code: int = 500, sub_code: int = 50001):
        ConsumerManager._assert_has_key(container, key, content_type)
        ConsumerManager._assert(
            isinstance(container[key], int) and container[key] >= 0,
            code=code,
            sub_code=sub_code,
            content_type=content_type,
            message=f"{key} must be a positive number"
        )

    @staticmethod
    def _assert_has_key(element: dict, key: str, content_type: str):
        ConsumerManager._assert(
            key in element, code=500, sub_code=50001, message=f"{key} missing from {element}", content_type=content_type
        )

    @staticmethod
    def _has_topic_and_partition_keys(topic_data: dict, content_type: str):
        for k in ["topic", "partition"]:
            ConsumerManager._assert_has_key(topic_data, k, content_type)

    @staticmethod
    def _topic_and_partition_valid(cluster_metadata: dict, topic_data: dict, content_type: str):
        ConsumerManager._has_topic_and_partition_keys(topic_data, content_type)
        topic = topic_data["topic"]
        partition = topic_data["partition"]
        if topic not in cluster_metadata["topics"]:
            KarapaceBase.not_found(message=f"Topic {topic} not found", content_type=content_type, sub_code=40401)
        partitions = {pi["partition"] for pi in cluster_metadata["topics"][topic]["partitions"]}
        if partition not in partitions:
            KarapaceBase.not_found(
                message=f"Partition {partition} not found for topic {topic}", content_type=content_type, sub_code=40402
            )

    @staticmethod
    def create_internal_name(group_name: str, consumer_name: str) -> Tuple[str, str]:
        return group_name, consumer_name

    @staticmethod
    def _validate_create_consumer(request: dict, content_type: str):
        consumer_data_valid = partial(ConsumerManager._assert, content_type=content_type, code=422, sub_code=42204)
        request["format"] = request.get("format", "binary")
        consumer_data_valid(request["format"] in KNOWN_FORMATS, message="Invalid format type")
        min_bytes_key = "fetch.min.bytes"
        consumer_data_valid(
            min_bytes_key not in request or isinstance(request[min_bytes_key], int) and request[min_bytes_key] >= -1,
            message=f"Expected {min_bytes_key} to be >= -1"
        )
        auto_reset_key = "auto.offset.reset"
        consumer_data_valid(
            cond=auto_reset_key not in request or request[auto_reset_key].lower() in OFFSET_RESET_STRATEGIES,
            message=f"Invalid value bar for configuration {auto_reset_key}: "
            f"String must be one of: {OFFSET_RESET_STRATEGIES}"
        )

    @staticmethod
    def _illegal_state_fail(message: str, content_type: str):
        return ConsumerManager._assert(cond=False, code=409, sub_code=40903, content_type=content_type, message=message)

    @staticmethod
    def _update_partition_assignments(consumer: KafkaConsumer):
        # This is (should be?) equivalent to calling poll on the consumer.
        # which would return 0 results, since the subscription we just created will mean
        # a rejoin is needed, which skips the actual fetching. Nevertheless, an actual call to poll is to be avoided
        # and a better solution to this is desired (extend the consumer??)
        # pylint: disable=W0212
        consumer._coordinator.poll()
        if not consumer._subscription.has_all_fetch_positions():
            consumer._update_fetch_positions(consumer._subscription.missing_fetch_positions())

    # external api below
    # CONSUMER
    async def create_consumer(self, group_name: str, request_data: dict, content_type: str):
        group_name = group_name.strip("/")
        self.log.info("Create consumer request for group  %s", group_name)
        consumer_name = request_data.get("name") or self.new_name()
        internal_name = self.create_internal_name(group_name, consumer_name)
        async with self.consumer_locks[internal_name]:
            if internal_name in self.consumers:
                self.log.error("Error creating duplicate consumer in group %s with id %s", group_name, consumer_name)
                KarapaceBase.r(
                    status=409,
                    content_type=content_type,
                    body={
                        "error_code": 40902,
                        "message": f"Consumer {consumer_name} already exists"
                    }
                )
            self._validate_create_consumer(request_data, content_type)
            self.log.info(
                "Creating new consumer in group %s with id %s and request_info %r", group_name, consumer_name, request_data
            )
            for k in ["consumer.request.timeout.ms", "fetch_min_bytes"]:
                convert_to_int(request_data, k, content_type)
            try:
                enable_commit = request_data.get("auto.commit.enable", self.config["consumer_enable_auto_commit"])
                if isinstance(enable_commit, str):
                    enable_commit = enable_commit.lower() == "true"
                request_data["consumer.request.timeout.ms"] = request_data.get(
                    "consumer.request.timeout.ms", self.config["consumer_request_timeout_ms"]
                )
                request_data["auto.commit.enable"] = enable_commit
                request_data["auto.offset.reset"] = request_data.get("auto.offset.reset", "earliest")
                fetch_min_bytes = request_data.get("fetch.min.bytes", self.config["fetch_min_bytes"])
                c = await self.create_kafka_consumer(fetch_min_bytes, group_name, internal_name, request_data)
            except KafkaConfigurationError as e:
                KarapaceBase.internal_error(str(e), content_type)
            self.consumers[internal_name] = TypedConsumer(
                consumer=c, serialization_format=request_data["format"], config=request_data
            )
            base_uri = urljoin(self.hostname, f"consumers/{group_name}/instances/{consumer_name}")
            KarapaceBase.r(content_type=content_type, body={"base_uri": base_uri, "instance_id": consumer_name})

    async def create_kafka_consumer(self, fetch_min_bytes, group_name, internal_name, request_data):
        while True:
            try:
                c = KafkaConsumer(
                    bootstrap_servers=self.config["bootstrap_uri"],
                    client_id=internal_name,
                    security_protocol=self.config["security_protocol"],
                    ssl_cafile=self.config["ssl_cafile"],
                    ssl_certfile=self.config["ssl_certfile"],
                    ssl_keyfile=self.config["ssl_keyfile"],
                    group_id=group_name,
                    fetch_min_bytes=fetch_min_bytes,
                    fetch_max_bytes=self.config["consumer_request_max_bytes"],
                    request_timeout_ms=request_data["consumer.request.timeout.ms"],
                    enable_auto_commit=request_data["auto.commit.enable"],
                    auto_offset_reset=request_data["auto.offset.reset"]
                )
                return c
            except:  # pylint: disable=bare-except
                self.log.exception("Unable to create consumer, retrying")
                await asyncio.sleep(1)

    async def delete_consumer(self, internal_name: Tuple[str, str], content_type: str):
        self.log.info("Deleting consumer for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        async with self.consumer_locks[internal_name]:
            try:
                c = self.consumers.pop(internal_name)
                c.consumer.close()
                self.consumer_locks.pop(internal_name)
            except:  # pylint: disable=bare-except
                self.log.exception("Unable to properly dispose of consumer")
            finally:
                empty_response()

    # OFFSETS
    async def commit_offsets(
        self, internal_name: Tuple[str, str], content_type: str, request_data: dict, cluster_metadata: dict
    ):
        self.log.info("Committing offsets for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        if request_data:
            self._assert_has_key(request_data, "offsets", content_type)
        payload = {}
        for el in request_data.get("offsets", []):
            for k in ["partition", "offset"]:
                convert_to_int(el, k, content_type)
            # If we commit for a partition that does not belong to this consumer, then the internal error raised
            # is marked as retriable, and thus the commit method will remain blocked in what looks like an infinite loop
            self._topic_and_partition_valid(cluster_metadata, el, content_type)
            payload[TopicPartition(el["topic"], el["partition"])] = OffsetAndMetadata(el["offset"] + 1, None)

        async with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            payload = payload or None
            try:
                consumer.commit(offsets=payload)
            except KafkaError as e:
                KarapaceBase.internal_error(message=f"error sending commit request: {e}", content_type=content_type)
        empty_response()

    async def get_offsets(self, internal_name: Tuple[str, str], content_type: str, request_data: dict):
        self.log.info("Retrieving offsets for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        self._assert_has_key(request_data, "partitions", content_type)
        response = {"offsets": []}
        async with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            for el in request_data["partitions"]:
                convert_to_int(el, "partition", content_type)
                tp = TopicPartition(el["topic"], el["partition"])
                commit_info = consumer.committed(tp, metadata=True)
                if not commit_info:
                    continue
                response["offsets"].append({
                    "topic": tp.topic,
                    "partition": tp.partition,
                    "metadata": commit_info.metadata,
                    "offset": commit_info.offset
                })
        KarapaceBase.r(body=response, content_type=content_type)

    # SUBSCRIPTION
    async def set_subscription(self, internal_name: Tuple[str, str], content_type: str, request_data: dict):
        self.log.info("Updating subscription for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        topics = request_data.get("topics", [])
        topics_pattern = request_data.get("topic_pattern")
        async with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            try:
                consumer.subscribe(topics=topics, pattern=topics_pattern)
                self._update_partition_assignments(consumer)
                empty_response()
            except AssertionError:
                self._illegal_state_fail(
                    message="Neither topic_pattern nor topics are present in request", content_type=content_type
                )
            except IllegalStateError as e:
                self._illegal_state_fail(str(e), content_type=content_type)
            finally:
                self.log.info("Done updating subscription")

    async def get_subscription(self, internal_name: Tuple[str, str], content_type: str):
        self.log.info("Retrieving subscription for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        async with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            if consumer.subscription() is None:
                topics = []
            else:
                topics = list(consumer.subscription())
            KarapaceBase.r(content_type=content_type, body={"topics": topics})

    async def delete_subscription(self, internal_name: Tuple[str], content_type: str):
        self.log.info("Deleting subscription for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        async with self.consumer_locks[internal_name]:
            self.consumers[internal_name].consumer.unsubscribe()
        empty_response()

    # ASSIGNMENTS
    async def set_assignments(self, internal_name: Tuple[str, str], content_type: str, request_data: dict):
        self.log.info("Updating assignments for %s to %r", internal_name, request_data)
        self._assert_consumer_exists(internal_name, content_type)
        self._assert_has_key(request_data, "partitions", content_type)
        partitions = []
        for el in request_data["partitions"]:
            convert_to_int(el, "partition", content_type)
            self._has_topic_and_partition_keys(el, content_type)
            partitions.append(TopicPartition(el["topic"], el["partition"]))
        async with self.consumer_locks[internal_name]:
            try:
                consumer = self.consumers[internal_name].consumer
                consumer.assign(partitions)
                self._update_partition_assignments(consumer)
                empty_response()
            except IllegalStateError as e:
                self._illegal_state_fail(message=str(e), content_type=content_type)
            finally:
                self.log.info("Done updating assignment")

    async def get_assignments(self, internal_name: Tuple[str, str], content_type: str):
        self.log.info("Retrieving assignment for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        async with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            KarapaceBase.r(
                content_type=content_type,
                body={"partitions": [{
                    "topic": pd.topic,
                    "partition": pd.partition
                } for pd in consumer.assignment()]}
            )

    # POSITIONS
    async def seek_to(self, internal_name: Tuple[str, str], content_type: str, request_data: dict):
        self.log.info("Resetting offsets for %s to %r", internal_name, request_data)
        self._assert_consumer_exists(internal_name, content_type)
        self._assert_has_key(request_data, "offsets", content_type)
        seeks = []
        for el in request_data["offsets"]:
            self._assert_has_key(el, "topic", content_type)
            for k in ["offset", "partition"]:
                self._assert_has_key(el, k, content_type)
                convert_to_int(el, k, content_type)
            self._assert_positive_number(el, "offset", content_type)
            seeks.append((TopicPartition(topic=el["topic"], partition=el["partition"]), el["offset"]))
        async with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            for part, offset in seeks:
                try:
                    consumer.seek(part, offset)
                except AssertionError:
                    self._illegal_state_fail(f"Partition {part} is unassigned", content_type)
            empty_response()

    async def seek_limit(
        self, internal_name: Tuple[str, str], content_type: str, request_data: dict, beginning: bool = True
    ):
        direction = "beginning" if beginning else "end"
        self.log.info("Seeking %s offsets", direction)
        self._assert_consumer_exists(internal_name, content_type)
        self._assert_has_key(request_data, "partitions", content_type)
        resets = []
        for el in request_data["partitions"]:
            convert_to_int(el, "partition", content_type)
            for k in ["topic", "partition"]:
                self._assert_has_key(el, k, content_type)
            resets.append(TopicPartition(topic=el["topic"], partition=el["partition"]))

        async with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            try:
                if beginning:
                    consumer.seek_to_beginning(*resets)
                else:
                    consumer.seek_to_end(*resets)
                empty_response()
            except AssertionError:
                self._illegal_state_fail(f"Trying to reset unassigned partitions to {direction}", content_type)

    async def fetch(self, internal_name: Tuple[str, str], content_type: str, formats: dict, query_params: dict):
        self.log.info("Running fetch for name %s with parameters %r and formats %r", internal_name, query_params, formats)
        self._assert_consumer_exists(internal_name, content_type)
        async with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            serialization_format = self.consumers[internal_name].serialization_format
            config = self.consumers[internal_name].config
            request_format = formats["embedded_format"]
            self._assert(
                cond=serialization_format == request_format,
                code=406,
                sub_code=40601,
                content_type=content_type,
                message=f"Consumer format {serialization_format} does not match the embedded format {request_format}"
            )
            self.log.info("Fetch request for %s with params %r", internal_name, query_params)
            try:
                timeout = int(query_params["timeout"]) if "timeout" in query_params \
                    else config["consumer.request.timeout.ms"]
                # we get to be more in line with the confluent proxy by doing a bunch of fetches each time and
                # respecting the max fetch request size
                max_bytes = int(query_params['max_bytes']) if "max_bytes" in query_params \
                    else consumer.config["fetch_max_bytes"]
            except ValueError:
                KarapaceBase.internal_error(message=f"Invalid request parameters: {query_params}", content_type=content_type)
            for val in [timeout, max_bytes]:
                if not val:
                    continue
                if val <= 0:
                    KarapaceBase.internal_error(message=f"Invalid request parameter {val}", content_type=content_type)
            response = []
            self.log.info(
                "Will poll multiple times for a single message with a total timeout of %dms, "
                "until at least %d bytes have been fetched", timeout, max_bytes
            )
            read_bytes = 0
            start_time = time.monotonic()
            poll_data = defaultdict(list)
            message_count = 0
            while read_bytes < max_bytes and start_time + timeout / 1000 > time.monotonic():
                time_left = start_time + timeout / 1000 - time.monotonic()
                bytes_left = max_bytes - read_bytes
                self.log.info(
                    "Polling with %r time left and %d bytes left, gathered %d messages so far", time_left, bytes_left,
                    message_count
                )
                data = consumer.poll(timeout_ms=timeout, max_records=1)
                self.log.debug("Successfully polled for messages")
                for topic, records in data.items():
                    for rec in records:
                        message_count += 1
                        read_bytes += \
                            max(0, rec.serialized_key_size) + \
                            max(0, rec.serialized_value_size) + \
                            max(0, rec.serialized_header_size)
                        poll_data[topic].append(rec)
            self.log.info("Gathered %d total messages", message_count)
            for tp in poll_data:
                for msg in poll_data[tp]:
                    try:
                        key = await self.deserialize(msg.key, request_format)
                        value = await self.deserialize(msg.value, request_format)
                    except (UnpackError, InvalidMessageHeader, InvalidPayload) as e:
                        KarapaceBase.internal_error(message=f"deserialization error: {e}", content_type=content_type)
                    element = {
                        "topic": tp.topic,
                        "partition": tp.partition,
                        "offset": msg.offset,
                        "key": key,
                        "value": value,
                    }
                    response.append(element)

            KarapaceBase.r(content_type=content_type, body=response)

    async def deserialize(self, bytes_: bytes, fmt: str):
        if not bytes_:
            return None
        if fmt == "avro":
            return await self.deserializer.deserialize(bytes_)
        if fmt == "json":
            return json.loads(bytes_.decode('utf-8'))
        return base64.b64encode(bytes_).decode('utf-8')

    def close(self):
        for k in list(self.consumers.keys()):
            c = self.consumers.pop(k)
            try:
                c.consumer.close()
            except:  # pylint: disable=bare-except
                pass
