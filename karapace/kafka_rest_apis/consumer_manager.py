from asyncio import Lock
from collections import defaultdict, namedtuple
from functools import partial
from http import HTTPStatus
from kafka import KafkaConsumer
from kafka.errors import IllegalStateError, KafkaConfigurationError, KafkaError
from kafka.structs import OffsetAndMetadata, TopicPartition
from karapace.kafka_rest_apis.error_codes import RESTErrorCodes
from karapace.karapace import empty_response, KarapaceBase
from karapace.serialization import InvalidMessageHeader, InvalidPayload, SchemaRegistryDeserializer
from karapace.utils import convert_to_int
from struct import error as UnpackError
from typing import Tuple
from urllib.parse import urljoin

import asyncio
import base64
import logging
import time
import ujson
import uuid

KNOWN_FORMATS = {"json", "avro", "binary", "jsonschema", "protobuf"}
OFFSET_RESET_STRATEGIES = {"latest", "earliest"}

TypedConsumer = namedtuple("TypedConsumer", ["consumer", "serialization_format", "config"])
LOG = logging.getLogger(__name__)


def new_name() -> str:
    return str(uuid.uuid4())


class ConsumerManager:
    def __init__(self, config: dict) -> None:
        self.config = config
        if self.config["advertised_port"] is None:
            self.hostname = f"{self.config['advertised_protocol']}://{self.config['advertised_hostname']}:" \
                            f"{self.config['port']}"
        else:
            self.hostname = f"{self.config['advertised_protocol']}://{self.config['advertised_hostname']}:" \
                            f"{self.config['advertised_port']}"
        self.deserializer = SchemaRegistryDeserializer(config=config)
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
                message=f"Topic {topic} not found", content_type=content_type, sub_code=RESTErrorCodes.TOPIC_NOT_FOUND.value
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

    @staticmethod
    def _update_partition_assignments(consumer: KafkaConsumer):
        # This is (should be?) equivalent to calling poll on the consumer.
        # which would return 0 results, since the subscription we just created will mean
        # a rejoin is needed, which skips the actual fetching. Nevertheless, an actual call to poll is to be avoided
        # and a better solution to this is desired (extend the consumer??)

        # pylint: disable=protected-access
        consumer._coordinator.poll()
        if not consumer._subscription.has_all_fetch_positions():
            consumer._update_fetch_positions(consumer._subscription.missing_fetch_positions())
        # pylint: enable=protected-access

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
            base_uri = urljoin(self.hostname, f"consumers/{group_name}/instances/{consumer_name}")
            KarapaceBase.r(content_type=content_type, body={"base_uri": base_uri, "instance_id": consumer_name})

    async def create_kafka_consumer(self, fetch_min_bytes, group_name, internal_name, request_data):
        while True:
            try:
                session_timeout_ms = self.config["session_timeout_ms"]
                request_timeout_ms = max(
                    session_timeout_ms,
                    KafkaConsumer.DEFAULT_CONFIG["request_timeout_ms"],
                    request_data["consumer.request.timeout.ms"],
                )
                c = KafkaConsumer(
                    bootstrap_servers=self.config["bootstrap_uri"],
                    client_id=internal_name,
                    security_protocol=self.config["security_protocol"],
                    ssl_cafile=self.config["ssl_cafile"],
                    ssl_certfile=self.config["ssl_certfile"],
                    ssl_keyfile=self.config["ssl_keyfile"],
                    sasl_mechanism=self.config["sasl_mechanism"],
                    sasl_plain_username=self.config["sasl_plain_username"],
                    sasl_plain_password=self.config["sasl_plain_password"],
                    group_id=group_name,
                    fetch_min_bytes=fetch_min_bytes,
                    fetch_max_bytes=self.config["consumer_request_max_bytes"],
                    request_timeout_ms=request_timeout_ms,
                    enable_auto_commit=request_data["auto.commit.enable"],
                    auto_offset_reset=request_data["auto.offset.reset"],
                    session_timeout_ms=session_timeout_ms,
                )
                return c
            except:  # pylint: disable=bare-except
                LOG.exception("Unable to create consumer, retrying")
                await asyncio.sleep(1)

    async def delete_consumer(self, internal_name: Tuple[str, str], content_type: str):
        LOG.info("Deleting consumer for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        async with self.consumer_locks[internal_name]:
            try:
                c = self.consumers.pop(internal_name)
                c.consumer.close()
                self.consumer_locks.pop(internal_name)
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
        LOG.info("Retrieving offsets for %s", internal_name)
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
                response["offsets"].append(
                    {
                        "topic": tp.topic,
                        "partition": tp.partition,
                        "metadata": commit_info.metadata,
                        "offset": commit_info.offset,
                    }
                )
        KarapaceBase.r(body=response, content_type=content_type)

    # SUBSCRIPTION
    async def set_subscription(self, internal_name: Tuple[str, str], content_type: str, request_data: dict):
        LOG.info("Updating subscription for %s", internal_name)
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
                self._update_partition_assignments(consumer)
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
                except AssertionError:
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
                    consumer.seek_to_beginning(*resets)
                else:
                    consumer.seek_to_end(*resets)
                empty_response()
            except AssertionError:
                self._illegal_state_fail(f"Trying to reset unassigned partitions to {direction}", content_type)

    async def fetch(self, internal_name: Tuple[str, str], content_type: str, formats: dict, query_params: dict):
        LOG.info("Running fetch for name %s with parameters %r and formats %r", internal_name, query_params, formats)
        self._assert_consumer_exists(internal_name, content_type)
        async with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            serialization_format = self.consumers[internal_name].serialization_format
            config = self.consumers[internal_name].config
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
                    int(query_params["timeout"]) if "timeout" in query_params else config["consumer.request.timeout.ms"]
                )
                # we get to be more in line with the confluent proxy by doing a bunch of fetches each time and
                # respecting the max fetch request size
                max_bytes = (
                    int(query_params["max_bytes"]) if "max_bytes" in query_params else consumer.config["fetch_max_bytes"]
                )
            except ValueError:
                KarapaceBase.internal_error(message=f"Invalid request parameters: {query_params}", content_type=content_type)
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
            while read_bytes < max_bytes and start_time + timeout / 1000 > time.monotonic():
                time_left = start_time + timeout / 1000 - time.monotonic()
                bytes_left = max_bytes - read_bytes
                LOG.info(
                    "Polling with %r time left and %d bytes left, gathered %d messages so far",
                    time_left,
                    bytes_left,
                    message_count,
                )
                data = consumer.poll(timeout_ms=timeout, max_records=1)
                LOG.debug("Successfully polled for messages")
                for topic, records in data.items():
                    for rec in records:
                        message_count += 1
                        read_bytes += (
                            max(0, rec.serialized_key_size)
                            + max(0, rec.serialized_value_size)
                            + max(0, rec.serialized_header_size)
                        )
                        poll_data[topic].append(rec)
            LOG.info("Gathered %d total messages", message_count)
            for tp in poll_data:
                for msg in poll_data[tp]:
                    try:
                        key = await self.deserialize(msg.key, request_format) if msg.key else None
                        value = await self.deserialize(msg.value, request_format) if msg.value else None
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
        if fmt in {"avro", "jsonschema", "protobuf"}:
            return await self.deserializer.deserialize(bytes_)
        if fmt == "json":
            return ujson.loads(bytes_.decode("utf-8"))
        return base64.b64encode(bytes_).decode("utf-8")

    def close(self):
        for k in list(self.consumers.keys()):
            c = self.consumers.pop(k)
            try:
                c.consumer.close()
            except:  # pylint: disable=bare-except
                pass
