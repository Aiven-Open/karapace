"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener
from asyncio import Lock
from collections import defaultdict, namedtuple
from functools import partial
from http import HTTPStatus
from kafka.errors import GroupAuthorizationFailedError, IllegalStateError, KafkaConfigurationError, KafkaError
from kafka.structs import TopicPartition
from karapace.config import Config, create_client_ssl_context
from karapace.kafka_rest_apis.error_codes import RESTErrorCodes
from karapace.karapace import empty_response, KarapaceBase
from karapace.serialization import DeserializationError, InvalidMessageHeader, InvalidPayload, SchemaRegistrySerializer
from karapace.utils import convert_to_int, json_decode, JSONDecodeError
from struct import error as UnpackError
from typing import Tuple, List, Optional
from urllib.parse import urljoin

import asyncio
import base64
import logging
import time
import uuid

KNOWN_FORMATS = {"json", "avro", "binary", "jsonschema", "protobuf"}
OFFSET_RESET_STRATEGIES = {"latest", "earliest"}

TypedConsumer = namedtuple("TypedConsumer", ["consumer", "serialization_format", "config"])
LOG = logging.getLogger(__name__)


def new_name() -> str:
    return str(uuid.uuid4())


class TopicDeletionEventGenerator(ConsumerRebalanceListener):
    def __init__(self):
        # valid in the previous implementation, now is't used.
        # # pattern required since the AIOKafkaConsumer changes the configuration
        # # from regex to list of topics and vice-versa if a topic or a pattern it's added.
        # # during the restore of the partition I need to understand if we were using the pattern
        # # since isn't possible to get back the pattern registered from the consumer object.
        # self._pattern = pattern
        self._consumer = None

    def set_consumer(self, consumer: AIOKafkaConsumer):
        self._consumer = consumer

    # as documentations says we are guaranteed to be called when a topic is deleted
    async def on_partitions_revoked(self, revoked: List[TopicPartition]):
        assert self._consumer is not None

        available_topics = await self._consumer.topics()
        revoked_topics = [topic for topic, _ in revoked]
        for revoked_topic in revoked_topics:
            if revoked_topic not in available_topics:
                self._topic_deleted()
                break

    def _topic_deleted(self):
        self._consumer.unsubscribe()
        # this logic couldn't be performed because on how it's implemented the OIOKafka library.
        # if we assign manually the partition 1 of the topic A and the partition 3 of the topic B to a consumer
        # and the topic A for e.g. is deleted, this logic will assign manually all the partitions of topic B to the consumer
        # even if probably it's possible to construct by subtraction a direct call to subscribe specifying all the previous partitions
        # if the topic in the future will add new partitions we will lose all the other partitions, TLDR it's tricky to implement right
        # and maybe the best solution is simply to acknowledge the user of the error and unsubscribe the consumer when that case happens.
        #if self._pattern:
        #    self._consumer.unsubscribe()
        #    self._consumer.subscribe(topics=[], pattern=self._pattern, listener=self)
        #else:
        #    re_subscribe = self._consumer.subscription() - topic_name
        #    self._consumer.unsubscribe()
        #    self._consumer.subscribe(topics=list(re_subscribe), listener=self)

    def on_partitions_assigned(self, assigned: List[TopicPartition]):
        pass


class ConsumerManager:
    def __init__(self, config: Config, deserializer: SchemaRegistrySerializer) -> None:
        self.config = config
        self.base_uri = self.config["rest_base_uri"]
        self.deserializer = deserializer
        self.consumers = {}
        self.consumer_locks = defaultdict(Lock)

    @staticmethod
    def _assert(cond: bool, code: HTTPStatus, sub_code: int, message: str, content_type: str) -> None:
        if not cond:
            KarapaceBase.r(content_type=content_type, status=code, body={"message": message, "error_code": sub_code})

    def _assert_consumer_exists(self, internal_name: Tuple[str, str], content_type: str) -> None:
        if internal_name not in self.consumers:
            KarapaceBase.not_found(
                message=f"Consumer for {internal_name} not found among {list(self.consumers.keys())}",
                content_type=content_type,
                sub_code=RESTErrorCodes.CONSUMER_NOT_FOUND.value,
            )

    @staticmethod
    def _assert_positive_number(
        container: dict,
        key: str,
        content_type: str,
        code: HTTPStatus = HTTPStatus.INTERNAL_SERVER_ERROR,
        sub_code: int = RESTErrorCodes.INVALID_VALUE.value,
    ) -> None:
        ConsumerManager._assert_has_key(container, key, content_type)
        ConsumerManager._assert(
            isinstance(container[key], int) and container[key] >= 0,
            code=code,
            sub_code=sub_code,
            content_type=content_type,
            message=f"{key} must be a positive number",
        )

    @staticmethod
    def _assert_has_key(element: dict, key: str, content_type: str) -> None:
        ConsumerManager._assert(
            key in element,
            code=HTTPStatus.INTERNAL_SERVER_ERROR,
            sub_code=RESTErrorCodes.INVALID_VALUE.value,
            message=f"{key} missing from {element}",
            content_type=content_type,
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
            KarapaceBase.not_found(
                message=f"Topic {topic} not found", content_type=content_type,
                sub_code=RESTErrorCodes.TOPIC_NOT_FOUND.value
            )
        partitions = {pi["partition"] for pi in cluster_metadata["topics"][topic]["partitions"]}
        if partition not in partitions:
            KarapaceBase.not_found(
                message=f"Partition {partition} not found for topic {topic}",
                content_type=content_type,
                sub_code=RESTErrorCodes.PARTITION_NOT_FOUND.value,
            )

    @staticmethod
    def create_internal_name(group_name: str, consumer_name: str) -> Tuple[str, str]:
        return group_name, consumer_name

    @staticmethod
    def _validate_create_consumer(request: dict, content_type: str) -> None:
        consumer_data_valid = partial(
            ConsumerManager._assert,
            content_type=content_type,
            code=HTTPStatus.UNPROCESSABLE_ENTITY,
            sub_code=RESTErrorCodes.INVALID_CONSUMER_PARAMETERS.value,
        )
        request["format"] = request.get("format", "binary")
        consumer_data_valid(request["format"] in KNOWN_FORMATS, message="Invalid format type")
        min_bytes_key = "fetch.min.bytes"
        consumer_data_valid(
            min_bytes_key not in request or isinstance(request[min_bytes_key], int) and request[min_bytes_key] >= -1,
            message=f"Expected {min_bytes_key} to be >= -1",
        )
        auto_reset_key = "auto.offset.reset"
        consumer_data_valid(
            cond=auto_reset_key not in request or request[auto_reset_key].lower() in OFFSET_RESET_STRATEGIES,
            message=f"Invalid value bar for configuration {auto_reset_key}: "
                    f"String must be one of: {OFFSET_RESET_STRATEGIES}",
        )

    @staticmethod
    def _illegal_state_fail(message: str, content_type: str) -> None:
        ConsumerManager._assert(
            cond=False,
            code=HTTPStatus.CONFLICT,
            sub_code=RESTErrorCodes.ILLEGAL_STATE.value,
            content_type=content_type,
            message=message,
        )

    # external api below
    # CONSUMER
    async def create_consumer(self, group_name: str, request_data: dict, content_type: str):
        group_name = group_name.strip("/")
        consumer_name = request_data.get("name") or new_name()
        internal_name = self.create_internal_name(group_name, consumer_name)
        async with self.consumer_locks[internal_name]:
            if internal_name in self.consumers:
                LOG.warning(
                    "Error creating duplicate consumer in group %s with id %s",
                    group_name,
                    consumer_name,
                )
                KarapaceBase.r(
                    status=HTTPStatus.CONFLICT,
                    content_type=content_type,
                    body={
                        "error_code": RESTErrorCodes.CONSUMER_ALREADY_EXISTS.value,
                        "message": f"Consumer {consumer_name} already exists",
                    },
                )
            self._validate_create_consumer(request_data, content_type)
            for k in ["consumer.request.timeout.ms", "fetch_min_bytes"]:
                convert_to_int(request_data, k, content_type)
            LOG.info(
                "Creating new consumer in group. group name: %s consumer name: %s request_data %r",
                group_name,
                consumer_name,
                request_data,
            )
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
            consumer_base_uri = urljoin(self.base_uri, f"consumers/{group_name}/instances/{consumer_name}")
            KarapaceBase.r(content_type=content_type,
                           body={"base_uri": consumer_base_uri, "instance_id": consumer_name})

    async def create_kafka_consumer(self, fetch_min_bytes, group_name, internal_name, request_data) -> AIOKafkaConsumer:
        ssl_context = create_client_ssl_context(self.config)
        for retry in [True, True, False]:
            try:
                session_timeout_ms = self.config["session_timeout_ms"]
                request_timeout_ms = max(
                    session_timeout_ms,
                    305000,  # Copy of old default from kafka-python's request_timeout_ms (not exposed by aiokafka)
                    request_data["consumer.request.timeout.ms"],
                )
                c = AIOKafkaConsumer(
                    bootstrap_servers=self.config["bootstrap_uri"],
                    client_id=internal_name,
                    security_protocol=self.config["security_protocol"],
                    ssl_context=ssl_context,
                    sasl_mechanism=self.config["sasl_mechanism"],
                    sasl_plain_username=self.config["sasl_plain_username"],
                    sasl_plain_password=self.config["sasl_plain_password"],
                    group_id=group_name,
                    fetch_min_bytes=max(1, fetch_min_bytes),  # Discard earlier negative values
                    fetch_max_bytes=self.config["consumer_request_max_bytes"],
                    fetch_max_wait_ms=self.config.get("consumer_fetch_max_wait_ms", 500),
                    # Copy aiokafka default 500 ms
                    # This will cause delay if subscription is changed.
                    consumer_timeout_ms=self.config.get("consumer_timeout_ms", 200),  # Copy aiokafka default 200 ms
                    request_timeout_ms=request_timeout_ms,
                    enable_auto_commit=request_data["auto.commit.enable"],
                    auto_offset_reset=request_data["auto.offset.reset"],
                    session_timeout_ms=session_timeout_ms,
                )
                await c.start()
                return c
            except:  # pylint: disable=bare-except
                if retry:
                    LOG.exception("Unable to create consumer, retrying")
                else:
                    LOG.exception("Giving up after failing to create consumer")
                    raise
                await asyncio.sleep(1)

    async def delete_consumer(self, internal_name: Tuple[str, str], content_type: str):
        LOG.info("Deleting consumer for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        async with self.consumer_locks[internal_name]:
            try:
                c = self.consumers.pop(internal_name)
                await c.consumer.stop()
                await self.consumer_locks.pop(internal_name)
            except:  # pylint: disable=bare-except
                LOG.exception("Unable to properly dispose of consumer")
            finally:
                empty_response()

    # OFFSETS
    async def commit_offsets(
        self, internal_name: Tuple[str, str], content_type: str, request_data: dict, cluster_metadata: dict
    ):
        LOG.info("Committing offsets for %s", internal_name)
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
            payload[TopicPartition(el["topic"], el["partition"])] = el["offset"] + 1

        async with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            payload = payload or None
            try:
                await consumer.commit(offsets=payload)
            except KafkaError as e:
                KarapaceBase.internal_error(message=f"error sending commit request: {e}", content_type=content_type)
        empty_response()

    async def get_offsets(self, internal_name: Tuple[str, str], content_type: str, request_data: dict):
        LOG.info("Retrieving offsets for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        self._assert_has_key(request_data, "partitions", content_type)
        response = {"offsets": []}
        async with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            for el in request_data["partitions"]:
                convert_to_int(el, "partition", content_type)
                tp = TopicPartition(el["topic"], el["partition"])
                try:
                    offset = await consumer.committed(tp)
                except GroupAuthorizationFailedError:
                    KarapaceBase.r(body={"message": "Forbidden"}, content_type=content_type,
                                   status=HTTPStatus.FORBIDDEN)
                except KafkaError as ex:
                    KarapaceBase.internal_error(
                        message=f"Failed to get offsets: {ex}",
                        content_type=content_type,
                    )
                if offset is None:
                    continue
                response["offsets"].append(
                    {
                        "topic": tp.topic,
                        "partition": tp.partition,
                        "metadata": "",
                        "offset": offset,
                    }
                )
        KarapaceBase.r(body=response, content_type=content_type)

    # SUBSCRIPTION
    async def set_subscription(self, internal_name: Tuple[str, str], content_type: str, request_data: dict):
        LOG.info("Updating subscription for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        topics = request_data.get("topics", [])
        topics_pattern = request_data.get("topic_pattern")
        if not (topics or topics_pattern):
            self._illegal_state_fail(
                message="Neither topic_pattern nor topics are present in request", content_type=content_type
            )
        if topics and topics_pattern:
            self._illegal_state_fail(
                message="IllegalStateError: You must choose only one way to configure your consumer: "
                        "(1) subscribe to specific topics by name, (2) subscribe to topics matching a regex pattern, "
                        "(3) assign itself specific topic-partitions.",
                content_type=content_type,
            )
        async with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            try:
                # internally every time the deletion_listener it's replaced
                deletion_listener = TopicDeletionEventGenerator()
                # aiokafka does not verify access to topics subscribed during this call, thus cannot get topic authorzation
                # error immediately
                consumer.subscribe(topics=topics, pattern=topics_pattern, listener=deletion_listener)
                deletion_listener.set_consumer(consumer)
                empty_response()
            except IllegalStateError as e:
                self._illegal_state_fail(str(e), content_type=content_type)
            finally:
                LOG.info("Done updating subscription")

    async def get_subscription(self, internal_name: Tuple[str, str], content_type: str):
        LOG.info("Retrieving subscription for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        async with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            if consumer.subscription() is None:
                topics = []
            else:
                topics = list(consumer.subscription())
            KarapaceBase.r(content_type=content_type, body={"topics": topics})

    async def delete_subscription(self, internal_name: Tuple[str, str], content_type: str):
        LOG.info("Deleting subscription for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        async with self.consumer_locks[internal_name]:
            self.consumers[internal_name].consumer.unsubscribe()
        empty_response()

    # ASSIGNMENTS
    async def set_assignments(self, internal_name: Tuple[str, str], content_type: str, request_data: dict):
        LOG.info("Updating assignments for %s to %r", internal_name, request_data)
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
                empty_response()
            except IllegalStateError as e:
                self._illegal_state_fail(message=str(e), content_type=content_type)
            finally:
                LOG.info("Done updating assignment")

    async def get_assignments(self, internal_name: Tuple[str, str], content_type: str):
        LOG.info("Retrieving assignment for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        async with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            KarapaceBase.r(
                content_type=content_type,
                body={"partitions": [{"topic": pd.topic, "partition": pd.partition} for pd in consumer.assignment()]},
            )

    # POSITIONS
    async def seek_to(self, internal_name: Tuple[str, str], content_type: str, request_data: dict):
        LOG.info("Resetting offsets for %s to %r", internal_name, request_data)
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
                except IllegalStateError:
                    self._illegal_state_fail(f"Partition {part} is unassigned", content_type)
            empty_response()

    async def seek_limit(
        self, internal_name: Tuple[str, str], content_type: str, request_data: dict, beginning: bool = True
    ):
        direction = "beginning" if beginning else "end"
        LOG.info("Seeking %s offsets", direction)
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
                    await consumer.seek_to_beginning(*resets)
                else:
                    await consumer.seek_to_end(*resets)
                empty_response()
            except AssertionError:
                self._illegal_state_fail(f"Trying to reset unassigned partitions to {direction}", content_type)

    async def fetch(self, internal_name: Tuple[str, str], content_type: str, formats: dict, query_params: dict):
        LOG.info("Running fetch for name %s with parameters %r and formats %r", internal_name, query_params, formats)
        self._assert_consumer_exists(internal_name, content_type)
        async with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            serialization_format = self.consumers[internal_name].serialization_format
            consumer_config = self.consumers[internal_name].config
            request_format = formats["embedded_format"]
            self._assert(
                cond=serialization_format == request_format,
                code=HTTPStatus.NOT_ACCEPTABLE,
                sub_code=RESTErrorCodes.UNSUPPORTED_FORMAT.value,
                content_type=content_type,
                message=f"Consumer format {serialization_format} does not match the embedded format {request_format}",
            )
            LOG.info("Fetch request for %s with params %r", internal_name, query_params)
            try:
                timeout = (
                    int(query_params["timeout"])
                    if "timeout" in query_params
                    else consumer_config["consumer.request.timeout.ms"]
                )
                # we get to be more in line with the confluent proxy by doing a bunch of fetches each time and
                # respecting the max fetch request size
                # pylint: disable=protected-access
                max_bytes = int(query_params["max_bytes"]) if "max_bytes" in query_params else consumer._fetch_max_bytes
            except ValueError:
                KarapaceBase.internal_error(message=f"Invalid request parameters: {query_params}",
                                            content_type=content_type)
            for val in [timeout, max_bytes]:
                if not val:
                    continue
                if val <= 0:
                    KarapaceBase.internal_error(message=f"Invalid request parameter {val}", content_type=content_type)
            response = []
            LOG.info(
                "Will poll multiple times for a single message with a total timeout of %dms, "
                "until at least %d bytes have been fetched",
                timeout,
                max_bytes,
            )
            read_bytes = 0
            start_time = time.monotonic()
            poll_data = defaultdict(list)
            message_count = 0
            # Read buffered records with calling getmany() with possibly zero timeout and max_records=1 multiple times
            read_buffered = True
            while read_bytes < max_bytes and (start_time + timeout / 1000 > time.monotonic() or read_buffered):
                read_buffered = False
                time_left = start_time + timeout / 1000 - time.monotonic()
                bytes_left = max_bytes - read_bytes
                LOG.debug(
                    "Polling with %r time left and %d bytes left, gathered %d messages so far",
                    time_left,
                    bytes_left,
                    message_count,
                )
                timeout_left = max(0, (start_time - time.monotonic()) * 1000 + timeout)
                try:
                    data = await consumer.getmany(timeout_ms=timeout_left, max_records=1)
                except GroupAuthorizationFailedError:
                    KarapaceBase.r(body={"message": "Forbidden"}, content_type=content_type,
                                   status=HTTPStatus.FORBIDDEN)
                except KafkaError as ex:
                    KarapaceBase.internal_error(
                        message=f"Failed to fetch: {ex}",
                        content_type=content_type,
                    )
                LOG.debug("Successfully polled for messages")
                for topic, records in data.items():
                    for rec in records:
                        message_count += 1
                        read_bytes += max(0, 0 if rec.key is None else len(rec.key)) + max(
                            0, 0 if rec.value is None else len(rec.value)
                        )
                        poll_data[topic].append(rec)
                        read_buffered = True
            LOG.info(
                "Gathered %d total messages (%d bytes read) in %r",
                message_count,
                read_bytes,
                time.monotonic() - start_time,
            )
            for tp in poll_data:
                for msg in poll_data[tp]:
                    try:
                        key = await self.deserialize(msg.key, request_format) if msg.key else None
                    except DeserializationError as e:
                        KarapaceBase.unprocessable_entity(
                            message=f"key deserialization error for format {request_format}: {e}",
                            sub_code=RESTErrorCodes.HTTP_UNPROCESSABLE_ENTITY.value,
                            content_type=content_type,
                        )
                    try:
                        value = await self.deserialize(msg.value, request_format) if msg.value else None
                    except DeserializationError as e:
                        KarapaceBase.unprocessable_entity(
                            message=f"value deserialization error for format {request_format}: {e}",
                            sub_code=RESTErrorCodes.HTTP_UNPROCESSABLE_ENTITY.value,
                            content_type=content_type,
                        )
                    element = {
                        "topic": tp.topic,
                        "partition": tp.partition,
                        "offset": msg.offset,
                        "timestamp": msg.timestamp,
                        "key": key,
                        "value": value,
                    }
                    response.append(element)

            KarapaceBase.r(content_type=content_type, body=response)

    async def deserialize(self, bytes_: bytes, fmt: str):
        try:
            if not bytes_:
                return None
            if fmt in {"avro", "jsonschema", "protobuf"}:
                return await self.deserializer.deserialize(bytes_)
            if fmt == "json":
                return json_decode(bytes_)
            return base64.b64encode(bytes_).decode("utf-8")
        except (UnpackError, InvalidMessageHeader, InvalidPayload, JSONDecodeError, UnicodeDecodeError) as e:
            raise DeserializationError(e) from e

    async def aclose(self):
        for k in list(self.consumers.keys()):
            c = self.consumers.pop(k)
            try:
                await c.consumer.stop()
            except:  # pylint: disable=bare-except
                pass
