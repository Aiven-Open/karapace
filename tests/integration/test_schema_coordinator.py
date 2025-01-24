"""
karapace - schema coordinator

Copyright (c) 2024 Aiven Ltd
See LICENSE for details

Tests are adapted from aiokafka.tests.test_coordinator
"""

from __future__ import annotations

from aiokafka.client import AIOKafkaClient, ConnectionGroup, CoordinationType
from aiokafka.cluster import ClusterMetadata
from aiokafka.protocol.api import Response
from aiokafka.protocol.group import (
    HeartbeatRequest_v0 as HeartbeatRequest,
    JoinGroupRequest_v0 as JoinGroupRequest,
    LeaveGroupRequest_v0 as LeaveGroupRequest,
    SyncGroupResponse_v0 as SyncGroupResponse,
)
from aiokafka.util import create_future, create_task
from collections.abc import AsyncGenerator, Iterator
from karapace.core.coordinator.schema_coordinator import Assignment, SchemaCoordinator, SchemaCoordinatorGroupRebalance
from karapace.core.utils import json_encode
from karapace.version import __version__
from tenacity import retry, stop_after_delay, TryAgain, wait_fixed
from tests.integration.test_master_coordinator import AlwaysAvailableSchemaReaderStoppper
from tests.integration.utils.kafka_server import KafkaServers
from tests.utils import new_random_name
from typing import Final
from unittest import mock

import aiokafka.errors as Errors
import asyncio
import contextlib
import logging
import pytest
import time

UNKNOWN_MEMBER_ID = JoinGroupRequest.UNKNOWN_MEMBER_ID

LOG = logging.getLogger(__name__)

RETRY_TIME: Final = 20
RETRY_WAIT_SECONDS: Final = 0.5


@pytest.fixture(scope="function", name="mocked_client")
def fixture_mocked_aiokafka_client() -> Iterator[AIOKafkaClient]:
    mocked_client = mock.MagicMock(spec=AIOKafkaClient)
    mocked_client.cluster = ClusterMetadata()
    yield mocked_client


@pytest.fixture(scope="function", name="coordinator")
async def fixture_admin(
    loop: asyncio.AbstractEventLoop,
    mocked_client: AIOKafkaClient,
) -> AsyncGenerator:
    coordinator = SchemaCoordinator(
        mocked_client,
        AlwaysAvailableSchemaReaderStoppper(),
        "test-host",
        10101,
        "https",
        True,
        "highest",
        "test-group",
        retry_backoff_ms=10,
    )
    yield coordinator
    await coordinator.close()


async def _get_client(kafka_servers: KafkaServers) -> AIOKafkaClient:
    while True:
        try:
            client = AIOKafkaClient(bootstrap_servers=",".join(kafka_servers.bootstrap_servers))
            await client.bootstrap()
            break
        except Exception:
            LOG.exception("Kafka client bootstrap failed.")
            await asyncio.sleep(0.5)
    return client


@pytest.fixture(scope="function", name="client")
async def get_client(
    loop: asyncio.AbstractEventLoop,
    kafka_servers: KafkaServers,
) -> AsyncGenerator:
    client = await _get_client(kafka_servers)
    yield client
    await client.close()


@retry(stop=stop_after_delay(RETRY_TIME), wait=wait_fixed(RETRY_WAIT_SECONDS))
async def wait_for_ready(coordinator: SchemaCoordinator) -> None:
    if not coordinator.ready and coordinator.coordinator_id is None:
        raise TryAgain()


@retry(stop=stop_after_delay(RETRY_TIME), wait=wait_fixed(RETRY_WAIT_SECONDS))
async def wait_for_primary_state(coordinator: SchemaCoordinator) -> None:
    if not coordinator.are_we_master:
        raise TryAgain()
    await asyncio.sleep(0.1)


@pytest.mark.parametrize("primary_selection_strategy", ["highest", "lowest"])
async def test_coordinator_workflow(
    primary_selection_strategy: str,
    client: AIOKafkaClient,
    kafka_servers: KafkaServers,
) -> None:
    group_name = new_random_name("tg-")
    # Check if 2 coordinators will coordinate rebalances correctly
    # Check if the initial group join is performed correctly with minimal
    # setup

    waiting_time_before_acting_as_master_sec = 6
    coordinator = SchemaCoordinator(
        client,
        AlwaysAvailableSchemaReaderStoppper(),
        "test-host-1",
        10101,
        "https",
        True,
        primary_selection_strategy,
        group_name,
        session_timeout_ms=10000,
        heartbeat_interval_ms=500,
        retry_backoff_ms=100,
        # removing 1 second to consider the network latency in the rest of the test
        waiting_time_before_acting_as_master_ms=(waiting_time_before_acting_as_master_sec - 1) * 1000,
    )
    coordinator.start()
    assert coordinator.coordinator_id is None
    await wait_for_ready(coordinator)
    await coordinator.ensure_coordinator_known()
    assert coordinator.coordinator_id is not None

    assert not coordinator.are_we_master()
    # waiting a little bit more since the cluster needs to setup.
    await asyncio.sleep(2 * waiting_time_before_acting_as_master_sec)
    assert not coordinator.are_we_master(), "last fetch before being available as master"
    assert coordinator.are_we_master(), f"after {waiting_time_before_acting_as_master_sec} seconds we can act as a master"

    # Check if adding an additional coordinator will rebalance correctly
    client2 = await _get_client(kafka_servers=kafka_servers)
    coordinator2 = SchemaCoordinator(
        client2,
        AlwaysAvailableSchemaReaderStoppper(),
        "test-host-2",
        10100,
        "https",
        True,
        primary_selection_strategy,
        group_name,
        session_timeout_ms=10000,
        heartbeat_interval_ms=500,
        retry_backoff_ms=100,
    )
    coordinator2.start()
    assert coordinator2.coordinator_id is None

    await wait_for_ready(coordinator2)
    await coordinator2.ensure_coordinator_known()

    # Helper variables to distinguish the expected primary and secondary
    primary = coordinator2 if primary_selection_strategy == "highest" else coordinator
    primary_client = client2 if primary_selection_strategy == "highest" else client
    secondary = coordinator if primary_selection_strategy == "highest" else coordinator2
    secondary_client = client if primary_selection_strategy == "highest" else client2

    if primary == coordinator2:
        # we need to disable the master for `waiting_time_before_acting_as_master_sec` seconds each time,
        # a new node its elected as master.
        # if the coordinator its `coordinator1` since isn't changed we don't have to wait
        # for the `waiting_time_before_acting_as_master_sec` seconds.

        # give time to the election to be forwarded to all the coordinators.
        await asyncio.sleep(3)

        assert (
            not primary.are_we_master()
        ), "after a change in the coordinator we can act as a master until we wait for the required time"
        assert not secondary.are_we_master(), "also the second cannot be immediately a master"
        # after that time the primary can act as a master
        await asyncio.sleep(waiting_time_before_acting_as_master_sec)
        assert not primary.are_we_master(), "Last fetch before being available as master"
        assert not secondary.are_we_master(), "secondary cannot be master"

    assert primary.are_we_master()
    assert not secondary.are_we_master()

    # Check is closing the primary coordinator will rebalance the secondary to change to primary
    await primary.close()
    await primary_client.close()

    now = time.time()
    while time.time() - now < waiting_time_before_acting_as_master_sec:
        assert not secondary.are_we_master(), (
            f"Cannot become master before {waiting_time_before_acting_as_master_sec} seconds "
            f"for the late records that can arrive from the previous master"
        )
        await asyncio.sleep(0.5)

    attempts = 0
    while not secondary.are_we_master():
        attempts += 1
        if attempts >= 1000:
            raise ValueError("The master should have been elected")
        await asyncio.sleep(0.5)
    await secondary.close()
    await secondary_client.close()


async def test_failed_group_join(mocked_client: AIOKafkaClient, coordinator: SchemaCoordinator) -> None:
    coordinator.start()
    assert coordinator._coordination_task is not None
    assert coordinator._client is not None

    # disable for test
    coordinator._coordination_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await coordinator._coordination_task
    coordinator._coordination_task = create_task(asyncio.sleep(0.1))
    coordinator.coordinator_id = 15

    async def _on_join_leader_test(_: Response) -> bytes | None:
        return b"123"

    _on_join_leader_mock = mock.Mock()
    _on_join_leader_mock.side_effect = _on_join_leader_test

    async def do_rebalance() -> Assignment | None:
        rebalance = SchemaCoordinatorGroupRebalance(
            coordinator,
            coordinator.group_id,
            coordinator.coordinator_id,
            coordinator._session_timeout_ms,
            coordinator._retry_backoff_ms,
        )
        rebalance._on_join_leader = _on_join_leader_mock
        return await rebalance.perform_group_join()

    coordinator._client.api_version = (0, 10, 1)
    error_type = Errors.NoError

    async def send(*_, **__) -> JoinGroupRequest:
        resp = JoinGroupRequest.RESPONSE_TYPE(
            error_code=error_type.errno,
            generation_id=-1,  # generation_id
            group_protocol="sr",
            leader_id="111",  # leader_id
            member_id="111",  # member_id
            members=[],
        )
        return resp

    mocked_client.send.side_effect = send

    # Success case, joined successfully
    resp = await do_rebalance()
    assert resp == Assignment(member_id="sr", metadata=b"123")
    assert _on_join_leader_mock.call_count == 1

    # no exception expected, just wait
    error_type = Errors.GroupLoadInProgressError
    resp = await do_rebalance()
    assert resp is None
    assert coordinator.need_rejoin()

    error_type = Errors.InvalidGroupIdError
    with pytest.raises(Errors.InvalidGroupIdError):
        await do_rebalance()
    assert coordinator.need_rejoin()

    # no exception expected, member_id should be reset
    coordinator.member_id = "some_invalid_member_id"
    error_type = Errors.UnknownMemberIdError
    resp = await do_rebalance()
    assert resp is None
    assert coordinator.need_rejoin()
    assert coordinator.member_id == JoinGroupRequest.UNKNOWN_MEMBER_ID

    error_type = Errors.UnknownError()
    with pytest.raises(Errors.KafkaError):  # Masked as unknown error
        await do_rebalance()

    # no exception expected, coordinator_id should be reset
    error_type = Errors.GroupCoordinatorNotAvailableError
    resp = await do_rebalance()
    assert resp is None
    assert coordinator.need_rejoin()
    assert coordinator.coordinator_id is None
    coordinator.coordinator_id = 15
    coordinator._coordinator_dead_fut = create_future()

    async def _on_join_leader(_) -> bytes | None:
        return None

    # Sync group fails case
    error_type = Errors.NoError
    _on_join_leader_mock.side_effect = _on_join_leader
    resp = await do_rebalance()
    assert coordinator.coordinator_id == 15
    assert resp is None
    assert _on_join_leader_mock.call_count == 2

    # `_send_req` itself raises an error
    mocked_client.send.side_effect = Errors.GroupCoordinatorNotAvailableError()
    resp = await do_rebalance()
    assert resp is None
    assert coordinator.need_rejoin()
    assert coordinator.coordinator_id is None


async def test_failed_sync_group(mocked_client: AIOKafkaClient, coordinator: SchemaCoordinator) -> None:
    coordinator.start()
    assert coordinator._coordination_task is not None
    assert coordinator._client is not None

    # disable for test
    coordinator._coordination_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await coordinator._coordination_task
    coordinator._coordination_task = create_task(asyncio.sleep(0.1))
    coordinator.coordinator_id = 15

    async def do_sync_group() -> bytes | None:
        rebalance = SchemaCoordinatorGroupRebalance(
            coordinator,
            coordinator.group_id,
            coordinator.coordinator_id,
            coordinator._session_timeout_ms,
            coordinator._retry_backoff_ms,
        )
        await rebalance._on_join_follower()

    coordinator._client.api_version = (0, 10, 1)
    error_type = None

    async def send(*_, **__) -> SyncGroupResponse:
        resp = SyncGroupResponse(error_code=error_type.errno, member_assignment=b"123")
        return resp

    mocked_client.send.side_effect = send

    coordinator.member_id = "some_invalid_member_id"

    error_type = Errors.RebalanceInProgressError
    await do_sync_group()
    assert coordinator.member_id == "some_invalid_member_id"
    assert coordinator.need_rejoin()

    error_type = Errors.UnknownMemberIdError
    await do_sync_group()
    assert coordinator.member_id == UNKNOWN_MEMBER_ID
    assert coordinator.need_rejoin()

    error_type = Errors.NotCoordinatorForGroupError
    await do_sync_group()
    assert coordinator.coordinator_id is None
    assert coordinator.need_rejoin()

    coordinator.coordinator_id = 15
    coordinator._coordinator_dead_fut = create_future()

    error_type = Errors.UnknownError()
    with pytest.raises(Errors.KafkaError):  # Masked as some KafkaError
        await do_sync_group()
    assert coordinator.need_rejoin()

    error_type = Errors.GroupAuthorizationFailedError()
    with pytest.raises(Errors.GroupAuthorizationFailedError) as exception_info:
        await do_sync_group()
    assert coordinator.need_rejoin()
    assert exception_info.value.args[0] == coordinator.group_id

    # If ``send()`` itself raises an error
    mocked_client.send.side_effect = Errors.GroupCoordinatorNotAvailableError()
    await do_sync_group()
    assert coordinator.need_rejoin()
    assert coordinator.coordinator_id is None


async def test_generation_change_during_rejoin_sync() -> None:
    client = mock.MagicMock(spec=AIOKafkaClient)
    coordinator = mock.MagicMock(spec=SchemaCoordinator)
    member_assignment = mock.Mock(spec=Assignment)

    coordinator._client = client
    coordinator._rebalance_timeout_ms = 1000
    coordinator._send_req = mock.MagicMock()

    rebalance = SchemaCoordinatorGroupRebalance(
        coordinator,
        "group_id",
        1,
        1000,
        1000,
    )

    async def send_req(_) -> Response:
        await asyncio.sleep(0.1)
        resp = mock.MagicMock(spec=Response)
        resp.member_assignment = member_assignment
        resp.error_code = 0
        return resp

    coordinator.send_req.side_effect = send_req

    request = mock.MagicMock()
    coordinator.generation = 1
    coordinator.member_id = "member_id"
    sync_req = asyncio.ensure_future(rebalance._send_sync_group_request(request))
    await asyncio.sleep(0.05)

    coordinator.generation = -1
    coordinator.member_id = "member_id-changed"

    assert await sync_req == member_assignment

    # make sure values are set correctly
    assert coordinator.generation == 1
    assert coordinator.member_id == "member_id"


async def test_coordinator_metadata_update(client: AIOKafkaClient) -> None:
    # Race condition where client.set_topics start MetadataUpdate, but it
    # fails to arrive before leader performed assignment
    try:
        coordinator = SchemaCoordinator(
            client,
            AlwaysAvailableSchemaReaderStoppper(),
            "test-host",
            10101,
            "https",
            True,
            "highest",
            "test-group",
            retry_backoff_ms=10,
        )
        coordinator.start()

        _metadata_update = client._metadata_update
        with mock.patch.object(client, "_metadata_update") as mocked:

            async def _new(*args, **kw) -> bool:
                # Just make metadata updates a bit more slow for test
                # robustness
                await asyncio.sleep(0.5)
                res = await _metadata_update(*args, **kw)
                return res

            mocked.side_effect = _new
            assignment_metadata = json_encode(
                {
                    "version": 2,
                    "karapace_version": __version__,
                    "host": "test-host",
                    "port": 10101,
                    "scheme": "https",
                    "master_eligibility": True,
                },
                binary=True,
                compact=True,
            )
            assert coordinator.get_metadata_snapshot() == [Assignment(member_id="v0", metadata=assignment_metadata)]
    finally:
        await coordinator.close()


async def test_coordinator__send_req(client: AIOKafkaClient) -> None:
    try:
        coordinator = SchemaCoordinator(
            client,
            AlwaysAvailableSchemaReaderStoppper(),
            "test-host",
            10101,
            "https",
            True,
            "highest",
            "test-group",
            session_timeout_ms=6000,
            heartbeat_interval_ms=1000,
        )
        coordinator.start()
        # Any request will do
        # We did not call ensure_coordinator_known yet
        request = LeaveGroupRequest()
        with pytest.raises(Errors.GroupCoordinatorNotAvailableError):
            await coordinator.send_req(request)

        await coordinator.ensure_coordinator_known()
        assert coordinator.coordinator_id is not None

        with mock.patch.object(client, "send") as mocked:

            async def mock_send(*_, **__) -> None:
                raise Errors.KafkaError("Some unexpected error")

            mocked.side_effect = mock_send

            # _send_req should mark coordinator dead on errors
            with pytest.raises(Errors.KafkaError):
                await coordinator.send_req(request)
            assert coordinator.coordinator_id is None
    finally:
        await coordinator.close()


async def test_coordinator_ensure_coordinator_known(client: AIOKafkaClient) -> None:
    try:
        coordinator = SchemaCoordinator(
            client,
            AlwaysAvailableSchemaReaderStoppper(),
            "test-host",
            10101,
            "https",
            True,
            "highest",
            "test-group",
            heartbeat_interval_ms=20000,
        )
        coordinator.start()
        assert coordinator._coordination_task is not None
        # disable for test
        coordinator._coordination_task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await coordinator._coordination_task
            coordinator._coordination_task = create_task(asyncio.sleep(0.1))

        def force_metadata_update() -> asyncio.Future:
            fut = create_future()
            fut.set_result(True)
            return fut

        client.ready = mock.Mock()
        client.force_metadata_update = mock.Mock()
        client.force_metadata_update.side_effect = force_metadata_update

        async def ready(node_id: int, group: ConnectionGroup) -> bool:
            if node_id == 0:
                return True
            return False

        client.ready.side_effect = ready
        client.coordinator_lookup = mock.Mock()

        coordinator_lookup: list | None = None

        async def _do_coordinator_lookup(_: CoordinationType, __: str) -> int:
            assert coordinator_lookup is not None
            node_id = coordinator_lookup.pop()
            if isinstance(node_id, Exception):
                raise node_id
            return node_id

        client.coordinator_lookup.side_effect = _do_coordinator_lookup

        # CASE: the lookup returns a broken node, that can't be connected
        # to. Ensure should wait until coordinator lookup finds the correct
        # node.
        coordinator.coordinator_dead()
        coordinator_lookup = [0, 1, 1]
        await coordinator.ensure_coordinator_known()
        assert coordinator.coordinator_id == 0
        assert client.force_metadata_update.call_count == 0

        # CASE: lookup fails with error first time. We update metadata and try
        # again
        coordinator.coordinator_dead()
        coordinator_lookup = [0, Errors.UnknownTopicOrPartitionError()]
        await coordinator.ensure_coordinator_known()
        assert client.force_metadata_update.call_count == 1

        # CASE: Special case for group authorization
        coordinator.coordinator_dead()
        coordinator_lookup = [0, Errors.GroupAuthorizationFailedError()]
        with pytest.raises(Errors.GroupAuthorizationFailedError) as exception_info:
            await coordinator.ensure_coordinator_known()
        assert exception_info.value.args[0] == coordinator.group_id

        # CASE: unretriable errors should be reraised to higher level
        coordinator.coordinator_dead()
        coordinator_lookup = [0, Errors.UnknownError()]
        with pytest.raises(Errors.UnknownError):
            await coordinator.ensure_coordinator_known()
    finally:
        await coordinator.close()


async def test_coordinator__do_heartbeat(client: AIOKafkaClient) -> None:
    try:
        coordinator = SchemaCoordinator(
            client,
            AlwaysAvailableSchemaReaderStoppper(),
            "test-host",
            10101,
            "https",
            True,
            "highest",
            "test-group",
            heartbeat_interval_ms=20000,
        )
        coordinator.start()
        assert coordinator._coordination_task is not None
        # disable for test
        coordinator._coordination_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await coordinator._coordination_task
        coordinator._coordination_task = create_task(asyncio.sleep(0.1))

        _orig_send_req = coordinator.send_req
        coordinator.send_req = mocked = mock.Mock()
        heartbeat_error = None
        send_req_error = None

        async def mock_send_req(request):
            if send_req_error is not None:
                raise send_req_error
            if request.API_KEY == HeartbeatRequest.API_KEY:
                if isinstance(heartbeat_error, list):
                    error_code = heartbeat_error.pop(0).errno
                else:
                    error_code = heartbeat_error.errno
                return HeartbeatRequest.RESPONSE_TYPE(error_code)
            return await _orig_send_req(request)

        mocked.side_effect = mock_send_req

        coordinator.coordinator_id = 15
        heartbeat_error = Errors.GroupCoordinatorNotAvailableError()
        success = await coordinator._do_heartbeat()
        assert not success
        assert coordinator.coordinator_id is None

        coordinator.rejoin_needed_fut = create_future()
        heartbeat_error = Errors.RebalanceInProgressError()
        success = await coordinator._do_heartbeat()
        assert success
        assert coordinator.rejoin_needed_fut.done()

        coordinator.member_id = "some_member"
        coordinator.rejoin_needed_fut = create_future()
        heartbeat_error = Errors.IllegalGenerationError()
        success = await coordinator._do_heartbeat()
        assert not success
        assert coordinator.rejoin_needed_fut.done()
        assert coordinator.member_id == UNKNOWN_MEMBER_ID

        coordinator.member_id = "some_member"
        coordinator.rejoin_needed_fut = create_future()
        heartbeat_error = Errors.UnknownMemberIdError()
        success = await coordinator._do_heartbeat()
        assert not success
        assert coordinator.rejoin_needed_fut.done()
        assert coordinator.member_id == UNKNOWN_MEMBER_ID

        heartbeat_error = Errors.GroupAuthorizationFailedError()
        with pytest.raises(Errors.GroupAuthorizationFailedError) as exception_info:
            await coordinator._do_heartbeat()
        assert exception_info.value.args[0] == coordinator.group_id

        heartbeat_error = Errors.UnknownError()
        with pytest.raises(Errors.KafkaError):
            await coordinator._do_heartbeat()

        heartbeat_error = None
        send_req_error = Errors.RequestTimedOutError()
        success = await coordinator._do_heartbeat()
        assert not success

        heartbeat_error = Errors.NoError()
        send_req_error = None
        success = await coordinator._do_heartbeat()
        assert success
    finally:
        await coordinator.close()


async def test_coordinator__heartbeat_routine(client: AIOKafkaClient) -> None:
    try:
        coordinator = SchemaCoordinator(
            client,
            AlwaysAvailableSchemaReaderStoppper(),
            "test-host",
            10101,
            "https",
            True,
            "highest",
            "test-group",
            heartbeat_interval_ms=100,
            session_timeout_ms=300,
            retry_backoff_ms=50,
        )
        coordinator.start()
        assert coordinator._coordination_task is not None
        # disable for test
        coordinator._coordination_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await coordinator._coordination_task
        coordinator._coordination_task = create_task(asyncio.sleep(0.1))

        mocked = mock.Mock()
        coordinator._do_heartbeat = mocked
        coordinator.coordinator_id = 15
        coordinator.member_id = 17
        coordinator.generation = 0
        success = False

        async def _do_heartbeat() -> bool:
            if isinstance(success, list):
                return success.pop(0)
            return success

        mocked.side_effect = _do_heartbeat

        async def ensure_coordinator_known() -> None:
            return None

        coordinator.ensure_coordinator_known = mock.Mock()
        coordinator.ensure_coordinator_known.side_effect = ensure_coordinator_known

        routine = create_task(coordinator._heartbeat_routine())

        # CASE: simple heartbeat
        success = True
        await asyncio.sleep(0.13)
        assert not routine.done()
        assert mocked.call_count == 1

        # CASE: 2 heartbeat fail
        success = False
        await asyncio.sleep(0.15)
        assert not routine.done()
        # We did 2 heartbeats as we waited only retry_backoff_ms between them
        assert mocked.call_count == 3

        # CASE: session_timeout_ms elapsed without heartbeat
        await asyncio.sleep(0.10)
        assert mocked.call_count == 5
        assert coordinator.coordinator_id == 15

        # last heartbeat try
        await asyncio.sleep(0.05)
        assert mocked.call_count == 6
        assert coordinator.coordinator_id is None
    finally:
        routine.cancel()
        await coordinator.close()


async def test_coordinator__coordination_routine(client: AIOKafkaClient) -> None:
    try:
        coordinator = SchemaCoordinator(
            client,
            AlwaysAvailableSchemaReaderStoppper(),
            "test-host",
            10101,
            "https",
            True,
            "highest",
            "test-group",
            heartbeat_interval_ms=20000,
            retry_backoff_ms=50,
        )

        def start_coordination():
            if coordinator._coordination_task:
                coordinator._coordination_task.cancel()
            coordinator._coordination_task = task = create_task(coordinator._coordination_routine())
            return task

        async def stop_coordination():
            if coordinator._coordination_task is not None:
                # disable for test
                coordinator._coordination_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await coordinator._coordination_task
            coordinator._coordination_task = create_task(asyncio.sleep(0.1))

        await stop_coordination()

        async def ensure_coordinator_known():
            return None

        coordinator.ensure_coordinator_known = coord_mock = mock.Mock()
        coord_mock.side_effect = ensure_coordinator_known

        coordinator._do_rejoin_group = rejoin_mock = mock.Mock()
        rejoin_ok = True

        async def do_rejoin():
            if rejoin_ok:
                coordinator.rejoin_needed_fut = create_future()
                return True
            await asyncio.sleep(0.1)
            return False

        rejoin_mock.side_effect = do_rejoin

        coordinator._start_heartbeat_task = mock.Mock()
        client.force_metadata_update = metadata_mock = mock.Mock()
        done_fut = create_future()
        done_fut.set_result(None)
        metadata_mock.side_effect = lambda: done_fut

        coordinator.rejoin_needed_fut = create_future()
        coordinator._closing = create_future()
        coordinator._coordinator_dead_fut = create_future()

        # CASE: coordination should coordinate and task get done
        # present
        task = start_coordination()
        await asyncio.sleep(0.01)
        assert not task.done()
        assert coord_mock.call_count == 1

        # CASE: with no assignment changes routine should not react to request_rejoin
        coordinator.request_rejoin()
        await asyncio.sleep(0.01)
        assert not task.done()
        assert coord_mock.call_count == 2
        assert rejoin_mock.call_count == 1

        # CASE: rejoin fail
        rejoin_ok = False
        coordinator.request_rejoin()
        await asyncio.sleep(0.01)
        assert not task.done()
        assert coord_mock.call_count == 3
        assert rejoin_mock.call_count == 2

        # CASE: After rejoin fail, retry
        rejoin_ok = True
        coordinator.request_rejoin()
        await asyncio.sleep(0.5)
        assert not task.done()
        assert coord_mock.call_count == 4
        assert rejoin_mock.call_count == 3

        # Close
        await coordinator.close()
        assert task.done()
    finally:
        await coordinator.close()
