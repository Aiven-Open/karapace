"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from karapace.kafka_rest_apis import ValidationCheckWrapper
from karapace.typing import TopicName
from unittest import mock
from unittest.mock import AsyncMock


async def test_validation_test_wrapper_elapse_works():
    with mock.patch("time.monotonic_ns") as mock_time:
        mock_time.return_value = 0

        mock_registry_client = AsyncMock()
        mock_registry_client.topic_require_validation.return_value = True

        wrapper = await ValidationCheckWrapper.construct_new(
            mock_registry_client,
            TopicName("test_topic"),
            cache_interval_ns=300,
        )

        result_1 = await wrapper.require_validation()
        mock_time.return_value = 200
        result_2 = await wrapper.require_validation()

        assert result_1 is True
        assert result_2 is True

        mock_registry_client.topic_require_validation.assert_called_once()


async def test_result_queried_twice_because_cache_evicted():
    with mock.patch("time.monotonic_ns") as mock_time:
        mock_time.return_value = 0

        mock_registry_client = AsyncMock()
        mock_registry_client.topic_require_validation.return_value = True

        wrapper = await ValidationCheckWrapper.construct_new(
            mock_registry_client,
            TopicName("test_topic"),
            cache_interval_ns=300,
        )

        result_1 = await wrapper.require_validation()
        mock_time.return_value = 301
        mock_registry_client.topic_require_validation.return_value = False
        result_2 = await wrapper.require_validation()

        assert result_1 is True
        assert result_2 is False

        assert mock_registry_client.topic_require_validation.call_count == 2
