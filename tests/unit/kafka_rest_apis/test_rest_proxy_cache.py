# pylint: disable=protected-access
"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from karapace.config import DEFAULTS
from karapace.kafka_rest_apis import UserRestProxy
from karapace.serialization import SchemaRegistrySerializer
from unittest.mock import patch

import copy


def user_rest_proxy(max_age_metadata: int = 5) -> UserRestProxy:
    configs = {**DEFAULTS, **{"admin_metadata_max_age": max_age_metadata}}
    serializer = SchemaRegistrySerializer(configs)
    return UserRestProxy(configs, 1, serializer, auth_expiry=None, verify_connection=False)


EMPTY_REPLY = {
    "topics": {},
    "brokers": [],
}

TOPIC_REQUEST = {
    "topics": {
        "topic_a": {
            "partitions": [
                {
                    "partition": 0,
                    "leader": 10,
                    "replicas": [
                        {"broker": 10, "leader": True, "in_sync": True},
                    ],
                }
            ]
        }
    },
    "brokers": [10],
}

ALL_TOPIC_REQUEST = {
    "topics": {
        "topic_a": {
            "partitions": [
                {
                    "partition": 0,
                    "leader": 69,
                    "replicas": [
                        {"broker": 69, "leader": True, "in_sync": True},
                        {"broker": 67, "leader": False, "in_sync": True},
                    ],
                }
            ]
        },
        "topic_b": {
            "partitions": [
                {
                    "partition": 0,
                    "leader": 66,
                    "replicas": [
                        {"broker": 69, "leader": False, "in_sync": True},
                        {"broker": 67, "leader": False, "in_sync": False},
                        {"broker": 66, "leader": True, "in_sync": True},
                        {"broker": 65, "leader": False, "in_sync": True},
                    ],
                }
            ]
        },
        "__consumer_offsets": {
            "partitions": [
                {
                    "partition": 0,
                    "leader": 69,
                    "replicas": [
                        {"broker": 69, "leader": True, "in_sync": True},
                        {"broker": 68, "leader": False, "in_sync": True},
                        {"broker": 67, "leader": False, "in_sync": True},
                    ],
                },
                {
                    "partition": 1,
                    "leader": 67,
                    "replicas": [
                        {"broker": 67, "leader": True, "in_sync": True},
                        {"broker": 68, "leader": False, "in_sync": True},
                        {"broker": 69, "leader": False, "in_sync": True},
                    ],
                },
                {
                    "partition": 2,
                    "leader": 67,
                    "replicas": [
                        {"broker": 67, "leader": True, "in_sync": True},
                        {"broker": 69, "leader": False, "in_sync": True},
                        {"broker": 68, "leader": False, "in_sync": True},
                    ],
                },
            ]
        },
    },
    "brokers": [68, 64, 66, 65, 69, 67],
}


async def test_cache_is_evicted_after_expiration_global_initially() -> None:
    proxy = user_rest_proxy()
    with patch(
        "karapace.kafka.admin.KafkaAdminClient.cluster_metadata", return_value=EMPTY_REPLY
    ) as mocked_cluster_metadata:
        await proxy.cluster_metadata(None)
    mocked_cluster_metadata.assert_called_once_with(None)  # "initially the metadata are always old"


async def test_cache_is_evicted_after_expiration_global() -> None:
    proxy = user_rest_proxy(max_age_metadata=10)
    proxy._global_metadata_birth = 0
    with patch(
        "karapace.kafka.admin.KafkaAdminClient.cluster_metadata", return_value=EMPTY_REPLY
    ) as mocked_cluster_metadata:
        with patch("time.monotonic", return_value=11):
            await proxy.cluster_metadata(None)
    mocked_cluster_metadata.assert_called_once_with(None)  # "metadata old require a refresh"


async def test_global_cache_is_used_for_single_topic() -> None:
    proxy = user_rest_proxy(max_age_metadata=10)
    proxy._global_metadata_birth = 0
    with patch(
        "karapace.kafka.admin.KafkaAdminClient.cluster_metadata", return_value=ALL_TOPIC_REQUEST
    ) as mocked_cluster_metadata:
        with patch("time.monotonic", return_value=11):
            await proxy.cluster_metadata(None)
            await proxy.cluster_metadata(None)
            await proxy.cluster_metadata(None)

    mocked_cluster_metadata.assert_called_once_with(None)  # "calling multiple times should be cached"

    assert proxy._global_metadata_birth == 11
    assert proxy._cluster_metadata_topic_birth == {"topic_a": 11, "topic_b": 11, "__consumer_offsets": 11}

    with patch(
        "karapace.kafka.admin.KafkaAdminClient.cluster_metadata", return_value=ALL_TOPIC_REQUEST
    ) as mocked_cluster_metadata:
        with patch("time.monotonic", return_value=14):
            await proxy.cluster_metadata(["topic_a", "topic_b"])

    assert (
        mocked_cluster_metadata.call_count == 0
    ), "the result should still be cached since we marked it as ready at time 11 and we are at 14"


async def test_cache_is_evicted_if_one_topic_is_expired() -> None:
    proxy = user_rest_proxy(max_age_metadata=10)
    proxy._global_metadata_birth = 0
    with patch(
        "karapace.kafka.admin.KafkaAdminClient.cluster_metadata", return_value=ALL_TOPIC_REQUEST
    ) as mocked_cluster_metadata:
        with patch("time.monotonic", return_value=11):
            await proxy.cluster_metadata(None)

    proxy._cluster_metadata_topic_birth = {"topic_a": 11, "topic_b": 1, "__consumer_offsets": 11}

    with patch(
        "karapace.kafka.admin.KafkaAdminClient.cluster_metadata", return_value=ALL_TOPIC_REQUEST
    ) as mocked_cluster_metadata:
        with patch("time.monotonic", return_value=14):
            await proxy.cluster_metadata(["topic_a", "topic_b"])

    assert mocked_cluster_metadata.call_count == 1, "topic_b should be evicted"


async def test_cache_is_evicted_if_a_topic_was_never_queries() -> None:
    proxy = user_rest_proxy(max_age_metadata=10)
    proxy._global_metadata_birth = 0
    with patch(
        "karapace.kafka.admin.KafkaAdminClient.cluster_metadata", return_value=ALL_TOPIC_REQUEST
    ) as mocked_cluster_metadata:
        with patch("time.monotonic", return_value=11):
            await proxy.cluster_metadata(None)

    proxy._cluster_metadata_topic_birth = {"topic_a": 11, "__consumer_offsets": 11}

    with patch(
        "karapace.kafka.admin.KafkaAdminClient.cluster_metadata", return_value=ALL_TOPIC_REQUEST
    ) as mocked_cluster_metadata:
        with patch("time.monotonic", return_value=14):
            await proxy.cluster_metadata(["topic_a", "topic_b"])

    assert mocked_cluster_metadata.call_count == 1, "topic_b is not present in the cache, should call the refresh"


async def test_cache_is_used_if_topic_requested_is_updated() -> None:
    proxy = user_rest_proxy(max_age_metadata=10)
    proxy._global_metadata_birth = 0
    with patch(
        "karapace.kafka.admin.KafkaAdminClient.cluster_metadata", return_value=TOPIC_REQUEST
    ) as mocked_cluster_metadata:
        with patch("time.monotonic", return_value=11):
            await proxy.cluster_metadata(None)

    with patch(
        "karapace.kafka.admin.KafkaAdminClient.cluster_metadata", return_value=ALL_TOPIC_REQUEST
    ) as mocked_cluster_metadata:
        with patch("time.monotonic", return_value=14):
            await proxy.cluster_metadata(["topic_a"])

    assert mocked_cluster_metadata.call_count == 0, "topic_a cache its present, should be used"


async def test_update_global_cache() -> None:
    proxy = user_rest_proxy(max_age_metadata=10)
    proxy._global_metadata_birth = 0
    with patch(
        "karapace.kafka.admin.KafkaAdminClient.cluster_metadata", return_value=TOPIC_REQUEST
    ) as mocked_cluster_metadata:
        with patch("time.monotonic", return_value=11):
            await proxy.cluster_metadata(None)

    assert mocked_cluster_metadata.call_count == 1, "should call the server for the first time"

    with patch(
        "karapace.kafka.admin.KafkaAdminClient.cluster_metadata", return_value=TOPIC_REQUEST
    ) as mocked_cluster_metadata:
        with patch("time.monotonic", return_value=21):
            await proxy.cluster_metadata(None)

    assert mocked_cluster_metadata.call_count == 0, "should call the server since the cache its expired"


async def test_update_topic_cache_do_not_evict_all_the_global_cache() -> None:
    proxy = user_rest_proxy(max_age_metadata=10)
    proxy._global_metadata_birth = 0
    proxy._cluster_metadata = ALL_TOPIC_REQUEST
    proxy._cluster_metadata_topic_birth = {"topic_a": 0, "topic_b": 200, "__consumer_offsets": 200}

    with patch(
        "karapace.kafka.admin.KafkaAdminClient.cluster_metadata", return_value=TOPIC_REQUEST
    ) as mocked_cluster_metadata:
        with patch("time.monotonic", return_value=208):
            res = await proxy.cluster_metadata(["topic_a"])

    assert res == TOPIC_REQUEST

    assert proxy._cluster_metadata_topic_birth == {"topic_a": 208, "topic_b": 200, "__consumer_offsets": 200}

    expected_metadata = copy.deepcopy(ALL_TOPIC_REQUEST)
    expected_metadata["topics"]["topic_a"] = TOPIC_REQUEST["topics"]["topic_a"]
    assert proxy._cluster_metadata == expected_metadata

    assert (
        mocked_cluster_metadata.call_count == 1
    ), "we should call the server since the previous time of caching for the topic_a was 0"
