"""
karapace - schema coordinator

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from aiokafka.client import AIOKafkaClient, ConnectionGroup
from aiokafka.cluster import ClusterMetadata
from aiokafka.consumer.group_coordinator import CoordinationType
from aiokafka.protocol.api import Request, Response
from aiokafka.protocol.commit import OffsetCommitRequest_v2 as OffsetCommitRequest
from aiokafka.protocol.group import (
    HeartbeatRequest,
    JoinGroupRequest,
    JoinGroupResponse_v0,
    JoinGroupResponse_v1,
    JoinGroupResponse_v2,
    JoinGroupResponse_v5,
    LeaveGroupRequest,
    SyncGroupRequest,
    SyncGroupRequest_v0,
    SyncGroupRequest_v1,
    SyncGroupRequest_v3,
)
from aiokafka.util import create_future, create_task
from karapace.dataclasses import default_dataclass
from karapace.typing import JsonData
from karapace.utils import json_decode, json_encode
from karapace.version import __version__
from typing import Any, Coroutine, Final, Sequence
from typing_extensions import TypedDict

import aiokafka.errors as Errors
import asyncio
import copy
import logging
import time

__all__ = ("SchemaCoordinator",)

# SR group errors
NO_ERROR: Final = 0

LOG = logging.getLogger(__name__)


class MemberIdentity(TypedDict):
    host: str
    port: int
    scheme: str
    master_eligibility: bool


class MemberAssignment(TypedDict):
    master: str
    master_identity: MemberIdentity


@default_dataclass
class JoinGroupMemberData:
    member_id: str
    member_data: bytes


@default_dataclass
class JoinGroupResponseData:
    leader_id: str
    protocol: str
    members: list[JoinGroupMemberData]


@default_dataclass
class Assignment:
    member_id: str
    metadata: bytes

    def to_tuple(self) -> tuple[str, bytes]:
        return (self.member_id, self.metadata)


def get_member_url(scheme: str, host: str, port: int) -> str:
    return f"{scheme}://{host}:{port}"


def get_member_configuration(*, host: str, port: int, scheme: str, master_eligibility: bool) -> JsonData:
    return {
        "version": 2,
        "karapace_version": __version__,
        "host": host,
        "port": port,
        "scheme": scheme,
        "master_eligibility": master_eligibility,
    }


@default_dataclass
class SchemaCoordinatorStatus:
    is_primary: bool | None
    is_primary_eligible: bool
    primary_url: str | None
    is_running: bool
    group_generation_id: int


SCHEMA_COORDINATOR_PROTOCOL: Final = "sr"


class SchemaCoordinator:
    """Schema registry specific group coordinator.

    Consumer group management is used to select primary Karapace
    from the Karapace cluster.

    This class is derived from aiokafka.consumer.group_coordinator.GroupCoordinator.
    Contains original comments and also Schema Registry specific comments.
    """

    are_we_master: bool | None = None
    master_url: str | None = None

    def __init__(
        self,
        client: AIOKafkaClient,
        hostname: str,
        port: int,
        scheme: str,
        master_eligibility: bool,
        election_strategy: str,
        group_id: str,
        heartbeat_interval_ms: int = 3000,
        max_poll_interval_ms: int = 300000,
        rebalance_timeout_ms: int = 30000,
        retry_backoff_ms: int = 100,
        session_timeout_ms: int = 10000,
    ) -> None:
        # Coordination flags and futures
        self._client: Final = client
        self._cluster: Final = client.cluster
        self._ready = False

        self.election_strategy: Final = election_strategy
        self.hostname: Final = hostname
        self.port: Final = port
        self.scheme: Final = scheme
        self.master_eligibility: Final = master_eligibility
        self.master_url: str | None = None
        self.are_we_master = False

        self.rejoin_needed_fut: asyncio.Future[None] | None = None
        self._coordinator_dead_fut: asyncio.Future[None] | None = None

        self.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID
        self.member_id = JoinGroupRequest[0].UNKNOWN_MEMBER_ID
        self.group_id: Final = group_id
        self.coordinator_id: int | None = None
        self.group_instance_id: str | None = None

        self._max_poll_interval: Final = max_poll_interval_ms / 1000
        self._heartbeat_interval_ms: Final = heartbeat_interval_ms
        self._rebalance_timeout_ms: Final = rebalance_timeout_ms
        self._retry_backoff_ms: Final = retry_backoff_ms
        self._session_timeout_ms: Final = session_timeout_ms

        self._coordinator_lookup_lock: Final = asyncio.Lock()
        self._coordination_task: asyncio.Future[None] | None = None

        # Will be started/stopped by coordination task
        self._heartbeat_task: asyncio.Task | None = None
        self._commit_refresh_task: asyncio.Task | None = None

        # Those are mostly unrecoverable exceptions, but user may perform an
        # action to handle those (for example add permission for this group).
        # Thus we set exception and pause coordination until user consumes it.
        self._pending_exception: BaseException | None = None
        self._error_consumed_fut: asyncio.Future | None = None

        # Will be set on close
        self._closing: asyncio.Future[None] | None = None

        self._metadata_snapshot: list[Assignment] = []

    def start(self) -> None:
        """Must be called after creating SchemaCoordinator object to initialize
        futures and start the coordination task.
        """
        self.rejoin_needed_fut = create_future()
        self._coordinator_dead_fut = create_future()
        self._closing = create_future()
        self._coordination_task = create_task(self._coordination_routine())

        # update initial subscription state using currently known metadata
        self._handle_metadata_update(self._cluster)
        self._cluster.add_listener(self._handle_metadata_update)

    def ready(self) -> bool:
        return self._ready

    async def send_req(self, request: Request) -> Response:
        """Send request to coordinator node. In case the coordinator is not
        ready a respective error will be raised.
        """
        node_id = self.coordinator_id
        if node_id is None:
            raise Errors.GroupCoordinatorNotAvailableError()
        try:
            resp = await self._client.send(node_id, request, group=ConnectionGroup.COORDINATION)
        except Errors.KafkaError as err:
            LOG.error(
                "Error sending %s to node %s [%s] -- marking coordinator dead", request.__class__.__name__, node_id, err
            )
            self.coordinator_dead()
            raise err
        return resp

    def check_errors(self) -> None:
        """Check if coordinator is well and no authorization or unrecoverable
        errors occurred
        """
        assert self._coordination_task is not None
        if self._coordination_task.done():
            self._coordination_task.result()
        if self._error_consumed_fut is not None:
            self._error_consumed_fut.set_result(None)
            self._error_consumed_fut = None
        if self._pending_exception is not None:
            exc = self._pending_exception
            self._pending_exception = None
            raise exc

    async def _push_error_to_user(self, exc: BaseException) -> Coroutine[Any, Any, tuple[set[Any], set[Any]]]:
        """Most critical errors are not something we can continue execution
        without user action. Well right now we just drop the Consumer, but
        java client would certainly be ok if we just poll another time, maybe
        it will need to rejoin, but not fail with GroupAuthorizationFailedError
        till the end of days...
        XXX: Research if we can't have the same error several times. For
             example if user gets GroupAuthorizationFailedError and adds
             permission for the group, would Consumer work right away or would
             still raise exception a few times?
        """
        exc = copy.copy(exc)
        self._pending_exception = exc
        self._error_consumed_fut = create_future()
        return asyncio.wait(
            [self._error_consumed_fut, self._closing],
            return_when=asyncio.FIRST_COMPLETED,
        )

    async def close(self) -> None:
        """Close the coordinator, leave the current group
        and reset local generation/memberId."""
        if self._closing is None or self._closing.done():
            return

        assert self._coordination_task is not None
        self._closing.set_result(None)
        # We must let the coordination task properly finish all pending work
        if not self._coordination_task.done():
            await self._coordination_task
        await self._stop_heartbeat_task()
        await self._maybe_leave_group()

    def maybe_leave_group(self) -> asyncio.Task:
        task = create_task(self._maybe_leave_group())
        return task

    async def _maybe_leave_group(self) -> None:
        if self.generation > 0 and self.group_instance_id is None:
            # this is a minimal effort attempt to leave the group. we do not
            # attempt any resending if the request fails or times out.
            # Note: do not send this leave request if we are running in static
            # partition assignment mode (when group_instance_id has been set).
            version = 0 if self._client.api_version < (0, 11, 0) else 1
            request = LeaveGroupRequest[version](self.group_id, self.member_id)
            try:
                await self.send_req(request)
            except Errors.KafkaError as err:
                LOG.error("LeaveGroup request failed: %s", err)
            else:
                LOG.info("LeaveGroup request succeeded")
        self.reset_generation()

    def _handle_metadata_update(self, _: ClusterMetadata) -> None:
        """Schema registry metadata update handler.

        Originally the metadata handler was defined in the
        aiokafka.consumer.group_coordinator.BaseCoordinator.
        """
        metadata_snapshot = self.get_metadata_snapshot()
        if self._metadata_snapshot != metadata_snapshot:
            LOG.info("Metadata for topic has changed from %s to %s. ", self._metadata_snapshot, metadata_snapshot)
            self._metadata_snapshot = metadata_snapshot
            self._on_metadata_change()

    def _on_metadata_change(self) -> None:
        """Schema registry specific behavior on metadata change is to request group rejoin."""
        self.request_rejoin()

    def get_metadata_snapshot(self) -> list[Assignment]:
        """Get schema registry specific metadata."""
        assert self.scheme is not None
        return [
            Assignment(
                member_id="v0",
                metadata=json_encode(
                    get_member_configuration(
                        host=self.hostname,
                        port=self.port,
                        scheme=self.scheme,
                        master_eligibility=self.master_eligibility,
                    ),
                    binary=True,
                    compact=True,
                ),
            )
        ]

    def _unpack_join_group_response(
        self,
        response: JoinGroupResponse_v0 | JoinGroupResponse_v1 | JoinGroupResponse_v2 | JoinGroupResponse_v5,
    ) -> JoinGroupResponseData:
        """Helper function to unpack the group join response data.

        The response versions are fixed to 0, 1, 2 and 5.
        See Kafka Protocol guide.
        """
        return JoinGroupResponseData(
            leader_id=response.leader_id,
            protocol=response.group_protocol,
            members=[JoinGroupMemberData(member_id=record[0], member_data=record[2]) for record in response.members],
        )

    async def perform_assignment(
        self,
        response: JoinGroupResponse_v0 | JoinGroupResponse_v1 | JoinGroupResponse_v2 | JoinGroupResponse_v5,
    ) -> Sequence[Assignment]:
        """Schema registry specific assignment handler.

        Selects the primary Karapace instance.
        This logic is run only on group leader instance.
        """
        response_data = self._unpack_join_group_response(response=response)
        LOG.info(
            "Creating assignment: %r, protocol: %r, members: %r",
            response_data.leader_id,
            response_data.protocol,
            response_data.members,
        )
        self.are_we_master = None
        error = NO_ERROR
        urls = {}
        fallback_urls = {}
        for member in response_data.members:
            member_identity = json_decode(member.member_data, MemberIdentity)
            if member_identity["master_eligibility"] is True:
                urls[get_member_url(member_identity["scheme"], member_identity["host"], member_identity["port"])] = (
                    member.member_id,
                    member.member_data,
                )
            else:
                fallback_urls[
                    get_member_url(member_identity["scheme"], member_identity["host"], member_identity["port"])
                ] = (member.member_id, member.member_data)
        if len(urls) > 0:
            chosen_url = sorted(urls, reverse=self.election_strategy.lower() == "highest")[0]
            schema_master_id, member_data = urls[chosen_url]
        else:
            # Protocol guarantees there is at least one member thus if urls is empty, fallback_urls cannot be
            chosen_url = sorted(fallback_urls, reverse=self.election_strategy.lower() == "highest")[0]
            schema_master_id, member_data = fallback_urls[chosen_url]
        member_identity = json_decode(member_data, MemberIdentity)
        identity = get_member_configuration(
            host=member_identity["host"],
            port=member_identity["port"],
            scheme=member_identity["scheme"],
            master_eligibility=member_identity["master_eligibility"],
        )
        LOG.info("Chose: %r with url: %r as the master", schema_master_id, chosen_url)

        assignments: list[Assignment] = []
        for member in response_data.members:
            member_data = json_encode(
                {"master": schema_master_id, "master_identity": identity, "error": error}, binary=True, compact=True
            )
            assignments.append(Assignment(member_id=member.member_id, metadata=member_data))
        return assignments

    async def _on_join_complete(
        self,
        generation: int,
        member_id: str,
        assignment: Assignment,
    ) -> None:
        """Schema registry specific handling of join complete.

        Sets the primary url and primary flag based on the assignment.
        Marks the SchemaCoordinator ready.
        """
        LOG.info(
            "Join complete, generation %r, member_id: %r, protocol: %r, member_assignment_bytes: %r",
            generation,
            member_id,
            assignment.member_id,
            assignment.metadata,
        )
        member_assignment = json_decode(assignment.metadata, MemberAssignment)
        member_identity = member_assignment["master_identity"]

        master_url = get_member_url(
            scheme=member_identity["scheme"],
            host=member_identity["host"],
            port=member_identity["port"],
        )
        # On Kafka protocol we can be assigned to be master, but if not master eligible, then we're not master for real
        if member_assignment["master"] == member_id and member_identity["master_eligibility"]:
            self.master_url = master_url
            self.are_we_master = True
        elif not member_identity["master_eligibility"]:
            self.master_url = None
            self.are_we_master = False
        else:
            self.master_url = master_url
            self.are_we_master = False
        self._ready = True
        return None

    def coordinator_dead(self) -> None:
        """Mark the current coordinator as dead.
        NOTE: this will not force a group rejoin. If new coordinator is able to
        recognize this member we will just continue with current generation.
        """
        if self._coordinator_dead_fut is not None and self.coordinator_id is not None:
            LOG.warning("Marking the coordinator dead (node %s)for group %s.", self.coordinator_id, self.group_id)
            self.coordinator_id = None
            self._coordinator_dead_fut.set_result(None)

    def reset_generation(self) -> None:
        """Coordinator did not recognize either generation or member_id. Will
        need to re-join the group.
        """
        self.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID
        self.member_id = JoinGroupRequest[0].UNKNOWN_MEMBER_ID
        self.request_rejoin()

    def request_rejoin(self) -> None:
        assert self.rejoin_needed_fut is not None
        if not self.rejoin_needed_fut.done():
            self.rejoin_needed_fut.set_result(None)

    def need_rejoin(self) -> bool:
        """Check whether the group should be rejoined

        Returns:
            bool: True if consumer should rejoin group, False otherwise
        """
        return self.rejoin_needed_fut is None or self.rejoin_needed_fut.done()

    async def ensure_coordinator_known(self) -> None:
        """Block until the coordinator for this group is known."""
        if self.coordinator_id is not None:
            return

        assert self._closing is not None
        async with self._coordinator_lookup_lock:
            retry_backoff = self._retry_backoff_ms / 1000
            while self.coordinator_id is None and not self._closing.done():
                try:
                    coordinator_id = await self._client.coordinator_lookup(CoordinationType.GROUP, self.group_id)
                except Errors.GroupAuthorizationFailedError as exc:
                    err = Errors.GroupAuthorizationFailedError(self.group_id)
                    raise err from exc
                except Errors.KafkaError as err:
                    LOG.error("Group Coordinator Request failed: %s", err)
                    if err.retriable:
                        await self._client.force_metadata_update()
                        await asyncio.sleep(retry_backoff)
                        continue
                    raise

                # Try to connect to confirm that the connection can be
                # established.
                ready = await self._client.ready(coordinator_id, group=ConnectionGroup.COORDINATION)
                if not ready:
                    await asyncio.sleep(retry_backoff)
                    continue

                self.coordinator_id = coordinator_id
                self._coordinator_dead_fut = create_future()
                LOG.info("Discovered coordinator %s for group %s", self.coordinator_id, self.group_id)

    async def _coordination_routine(self) -> None:
        try:
            await self.__coordination_routine()
        except asyncio.CancelledError:  # pylint: disable=try-except-raise
            raise
        except Exception as exc:
            LOG.error("Unexpected error in coordinator routine", exc_info=True)
            kafka_exc = Errors.KafkaError(f"Unexpected error during coordination {exc!r}")
            raise kafka_exc from exc

    async def __coordination_routine(self) -> None:
        """Main background task, that keeps track of changes in group
        coordination. This task will spawn/stop heartbeat task and perform
        autocommit in times it's safe to do so.
        """
        assert self._closing is not None
        assert self._coordinator_dead_fut is not None
        assert self.rejoin_needed_fut is not None
        while not self._closing.done():
            # Ensure active group
            try:
                await self.ensure_coordinator_known()
                if self.need_rejoin():
                    new_assignment = await self.ensure_active_group()
                    if not new_assignment:
                        continue
            except Errors.KafkaError as exc:
                # The ignore of returned coroutines need to be checked.
                # aiokafka also discards the return value
                await self._push_error_to_user(exc)  # type: ignore[unused-coroutine]
                continue

            futures = [
                self._closing,  # Will exit fast if close() called
                self._coordinator_dead_fut,
            ]
            # No assignments.
            # We don't want a heavy loop here.
            # NOTE: metadata changes are for partition count and pattern
            # subscription, which is irrelevant in case of user assignment.
            futures.append(self.rejoin_needed_fut)

            # We should always watch for other task raising critical or
            # unexpected errors, so we attach those as futures too. We will
            # check them right after wait.
            if self._heartbeat_task:
                futures.append(self._heartbeat_task)
            if self._commit_refresh_task:
                futures.append(self._commit_refresh_task)

            _, _ = await asyncio.wait(
                futures,
                return_when=asyncio.FIRST_COMPLETED,
            )

            # Handle exceptions in other background tasks
            for task in [self._heartbeat_task, self._commit_refresh_task]:
                if task and task.done():
                    task_exception = task.exception()
                    if task_exception:
                        # The ignore of returned coroutines need to be checked.
                        # aiokafka also discards the return value
                        await self._push_error_to_user(task_exception)  # type: ignore[unused-coroutine]

    async def ensure_active_group(self) -> bool:
        # due to a race condition between the initial metadata
        # fetch and the initial rebalance, we need to ensure that
        # the metadata is fresh before joining initially. This
        # ensures that we have matched the pattern against the
        # cluster's topics at least once before joining.
        # Also the rebalance can be issued by another node, that
        # discovered a new topic, which is still unknown to this
        # one.
        await self._client.force_metadata_update()

        # NOTE: we did not stop heartbeat task before to keep the
        # member alive during the callback, as it can commit offsets.
        # See the ``RebalanceInProgressError`` case in heartbeat
        # handling.
        await self._stop_heartbeat_task()

        # We will only try to perform the rejoin once. If it fails,
        # we will spin this loop another time, checking for coordinator
        # and subscription changes.
        # NOTE: We do re-join in sync. The group rebalance will fail on
        # subscription change and coordinator failure by itself and
        # this way we don't need to worry about racing or cancellation
        # issues that could occur if re-join were to be a task.
        success = await self._do_rejoin_group()
        if success:
            self._start_heartbeat_task()
            return True
        return False

    def _start_heartbeat_task(self) -> None:
        if self._heartbeat_task is None:
            self._heartbeat_task = create_task(self._heartbeat_routine())

    async def _stop_heartbeat_task(self) -> None:
        if self._heartbeat_task is not None:
            if not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
                await self._heartbeat_task
            self._heartbeat_task = None

    async def _heartbeat_routine(self) -> None:
        last_ok_heartbeat = time.monotonic()
        hb_interval = self._heartbeat_interval_ms / 1000
        session_timeout = self._session_timeout_ms / 1000
        retry_backoff = self._retry_backoff_ms / 1000
        sleep_time = hb_interval

        # There is no point to heartbeat after Broker stopped recognizing
        # this consumer, so we stop after resetting generation.
        while self.member_id != JoinGroupRequest[0].UNKNOWN_MEMBER_ID:
            try:
                await asyncio.sleep(sleep_time)
                await self.ensure_coordinator_known()

                t0 = time.monotonic()
                success = await self._do_heartbeat()
            except asyncio.CancelledError:
                break

            # NOTE: We let all other errors propagate up to coordination
            # routine

            if success:
                last_ok_heartbeat = time.monotonic()
                sleep_time = max((0, hb_interval - last_ok_heartbeat + t0))
            else:
                sleep_time = retry_backoff

            session_time = time.monotonic() - last_ok_heartbeat
            if session_time > session_timeout:
                # the session timeout has expired without seeing a successful
                # heartbeat, so we should probably make sure the coordinator
                # is still healthy.
                LOG.error("Heartbeat session expired - marking coordinator dead")
                self.coordinator_dead()

        LOG.debug("Stopping heartbeat task")

    async def _do_heartbeat(self) -> bool:
        version = 0 if self._client.api_version < (0, 11, 0) else 1
        request = HeartbeatRequest[version](self.group_id, self.generation, self.member_id)
        LOG.debug("Heartbeat: %s[%s] %s", self.group_id, self.generation, self.member_id)

        # _send_req may fail with error like `RequestTimedOutError`
        # we need to catch it so coordinator_routine won't fail
        try:
            resp = await self.send_req(request)
        except Errors.KafkaError as err:
            LOG.error("Heartbeat send request failed: %s. Will retry.", err)
            return False
        error_type = Errors.for_code(resp.error_code)
        if error_type is Errors.NoError:
            LOG.debug("Received successful heartbeat response for group %s", self.group_id)
            return True
        if error_type in (Errors.GroupCoordinatorNotAvailableError, Errors.NotCoordinatorForGroupError):
            LOG.warning(
                "Heartbeat failed for group %s: coordinator (node %s) is either not started or not valid",
                self.group_id,
                self.coordinator_id,
            )
            self.coordinator_dead()
        elif error_type is Errors.RebalanceInProgressError:
            LOG.warning("Heartbeat failed for group %s because it is rebalancing", self.group_id)
            self.request_rejoin()
            # it is valid to continue heartbeating while the group is
            # rebalancing. This ensures that the coordinator keeps the
            # member in the group for as long as the duration of the
            # rebalance timeout. If we stop sending heartbeats,
            # however, then the session timeout may expire before we
            # can rejoin.
            return True
        elif error_type is Errors.IllegalGenerationError:
            LOG.warning("Heartbeat failed for group %s: generation id is not current.", self.group_id)
            self.reset_generation()
        elif error_type is Errors.UnknownMemberIdError:
            LOG.warning("Heartbeat failed: local member_id was not recognized; resetting and re-joining group")
            self.reset_generation()
        elif error_type is Errors.GroupAuthorizationFailedError:
            raise error_type(self.group_id)
        else:
            kafka_error = Errors.KafkaError(f"Unexpected exception in heartbeat task: {error_type()!r}")
            LOG.error("Heartbeat failed: %r", kafka_error)
            raise kafka_error
        return False

    async def _do_rejoin_group(self) -> bool:
        rebalance = SchemaCoordinatorGroupRebalance(
            self,
            self.group_id,
            self.coordinator_id,
            self._session_timeout_ms,
            self._retry_backoff_ms,
        )
        assignment = await rebalance.perform_group_join()

        if assignment is None:
            # wait backoff and try again
            await asyncio.sleep(self._retry_backoff_ms / 1000)
            return False

        await self._on_join_complete(generation=self.generation, member_id=self.member_id, assignment=assignment)
        return True


class SchemaCoordinatorGroupRebalance:
    """ An adapter, that encapsulates rebalance logic
        On how to handle cases read in https://cwiki.apache.org/confluence/\
            display/KAFKA/Kafka+Client-side+Assignment+Proposal
    """

    def __init__(
        self,
        coordinator: SchemaCoordinator,
        group_id: str,
        coordinator_id: int | None,
        session_timeout_ms: int,
        retry_backoff_ms: int,
    ) -> None:
        self._coordinator: Final = coordinator
        self.group_id: Final = group_id
        self.coordinator_id: Final = coordinator_id
        self._session_timeout_ms: Final = session_timeout_ms
        self._retry_backoff_ms: Final = retry_backoff_ms
        self._api_version: Final = self._coordinator._client.api_version
        self._rebalance_timeout_ms: Final = self._coordinator._rebalance_timeout_ms

    async def perform_group_join(self) -> Assignment | None:
        """Join the group and return the assignment for the next generation.

        This function handles both JoinGroup and SyncGroup, delegating to
        perform_assignment() if elected as leader by the coordinator node.

        Returns encoded-bytes assignment returned from the group leader
        """
        # send a join group request to the coordinator
        LOG.info("(Re-)joining group %s", self.group_id)

        metadata_list = [assignment.to_tuple() for assignment in self._coordinator.get_metadata_snapshot()]
        # for KIP-394 we may have to send a second join request
        try_join = True
        while try_join:
            try_join = False

            if self._api_version < (0, 10, 1):
                request = JoinGroupRequest[0](
                    self.group_id,
                    self._session_timeout_ms,
                    self._coordinator.member_id,
                    SCHEMA_COORDINATOR_PROTOCOL,
                    metadata_list,
                )
            elif self._api_version < (0, 11, 0):
                request = JoinGroupRequest[1](
                    self.group_id,
                    self._session_timeout_ms,
                    self._rebalance_timeout_ms,
                    self._coordinator.member_id,
                    SCHEMA_COORDINATOR_PROTOCOL,
                    metadata_list,
                )
            elif self._api_version < (2, 3, 0):
                request = JoinGroupRequest[2](
                    self.group_id,
                    self._session_timeout_ms,
                    self._rebalance_timeout_ms,
                    self._coordinator.member_id,
                    SCHEMA_COORDINATOR_PROTOCOL,
                    metadata_list,
                )
            else:
                request = JoinGroupRequest[3](
                    self.group_id,
                    self._session_timeout_ms,
                    self._rebalance_timeout_ms,
                    self._coordinator.member_id,
                    self._coordinator.group_instance_id,
                    SCHEMA_COORDINATOR_PROTOCOL,
                    metadata_list,
                )

            # create the request for the coordinator
            LOG.debug("Sending JoinGroup (%s) to coordinator %s", request, self.coordinator_id)
            try:
                response = await self._coordinator.send_req(request)
            except Errors.KafkaError:
                # Return right away. It's a connection error, so backoff will be
                # handled by coordinator lookup
                return None

            error_type = Errors.for_code(response.error_code)

            if error_type is Errors.MemberIdRequired:
                self._coordinator.member_id = response.member_id
                try_join = True

        if error_type is Errors.NoError:
            LOG.debug("Join group response %s", response)
            self._coordinator.member_id = response.member_id
            self._coordinator.generation = response.generation_id
            protocol = response.group_protocol
            LOG.info(
                "Joined group '%s' (generation %s) with member_id %s",
                self.group_id,
                response.generation_id,
                response.member_id,
            )

            if response.leader_id == response.member_id:
                LOG.info("Elected group leader -- performing partition assignments using %s", protocol)
                assignment_bytes = await self._on_join_leader(response)
            else:
                assignment_bytes = await self._on_join_follower()

            if assignment_bytes is None:
                return None
            return Assignment(member_id=protocol, metadata=assignment_bytes)
        if error_type is Errors.GroupLoadInProgressError:
            # Backoff and retry
            LOG.debug(
                "Attempt to join group %s rejected since coordinator %s is loading the group.",
                self.group_id,
                self.coordinator_id,
            )
            await asyncio.sleep(self._retry_backoff_ms / 1000)
        elif error_type is Errors.UnknownMemberIdError:
            # reset the member id and retry immediately
            self._coordinator.reset_generation()
            LOG.debug("Attempt to join group %s failed due to unknown member id", self.group_id)
        elif error_type in (Errors.GroupCoordinatorNotAvailableError, Errors.NotCoordinatorForGroupError):
            # Coordinator changed we should be able to find it immediately
            err = error_type()
            self._coordinator.coordinator_dead()
            LOG.debug("Attempt to join group %s failed due to obsolete coordinator information: %s", self.group_id, err)
        elif error_type in (
            Errors.InconsistentGroupProtocolError,
            Errors.InvalidSessionTimeoutError,
            Errors.InvalidGroupIdError,
        ):
            err = error_type()
            LOG.error("Attempt to join group failed due to fatal error: %s", err)
            raise err
        elif error_type is Errors.GroupAuthorizationFailedError:
            raise error_type(self.group_id)
        else:
            err = error_type()
            LOG.error("Unexpected error in join group '%s' response: %s", self.group_id, err)
            raise Errors.KafkaError(repr(err))
        return None

    async def _on_join_follower(self) -> bytes | None:
        # send follower's sync group with an empty assignment
        LOG.info("Joined as follower.")
        if self._api_version < (2, 3, 0):
            version = 0 if self._api_version < (0, 11, 0) else 1
            request = SyncGroupRequest[version](
                self.group_id,
                self._coordinator.generation,
                self._coordinator.member_id,
                [],
            )
        else:
            request = SyncGroupRequest[2](
                self.group_id,
                self._coordinator.generation,
                self._coordinator.member_id,
                self._coordinator.group_instance_id,
                [],
            )
        LOG.debug(
            "Sending follower SyncGroup for group %s to coordinator %s: %s", self.group_id, self.coordinator_id, request
        )
        return await self._send_sync_group_request(request)

    async def _on_join_leader(self, response: JoinGroupResponse_v0) -> bytes | None:
        """
        Perform leader synchronization and send back the assignment
        for the group via SyncGroupRequest

        Arguments:
            response (JoinResponse): broker response to parse

        Returns:
            Future: resolves to member assignment encoded-bytes
        """
        try:
            group_assignment = await self._coordinator.perform_assignment(response)
        except Exception as e:
            raise Errors.KafkaError(repr(e))

        assignment_req = []
        for assignment in group_assignment:
            assignment_req.append((assignment.member_id, assignment.metadata))

        if self._api_version < (2, 3, 0):
            version = 0 if self._api_version < (0, 11, 0) else 1
            request = SyncGroupRequest[version](
                self.group_id,
                self._coordinator.generation,
                self._coordinator.member_id,
                assignment_req,
            )
        else:
            request = SyncGroupRequest[2](
                self.group_id,
                self._coordinator.generation,
                self._coordinator.member_id,
                self._coordinator.group_instance_id,
                assignment_req,
            )

        LOG.debug("Sending leader SyncGroup for group %s to coordinator %s: %s", self.group_id, self.coordinator_id, request)
        return await self._send_sync_group_request(request)

    async def _send_sync_group_request(
        self,
        request: SyncGroupRequest_v0 | SyncGroupRequest_v1 | SyncGroupRequest_v3,
    ) -> bytes | None:
        # We need to reset the rejoin future right after the assignment to
        # capture metadata changes after join group was performed. We do not
        # set it directly after JoinGroup to avoid a false rejoin in case
        # ``perform_assignment()`` does a metadata update.
        self._coordinator.rejoin_needed_fut = create_future()
        req_generation = self._coordinator.generation
        req_member_id = self._coordinator.member_id

        try:
            response = await self._coordinator.send_req(request)
        except Errors.KafkaError:
            # We lost connection to coordinator. No need to try and finish this
            # group join, just rejoin again.
            self._coordinator.request_rejoin()
            return None

        error_type = Errors.for_code(response.error_code)
        if error_type is Errors.NoError:
            LOG.info("Successfully synced group %s with generation %s", self.group_id, self._coordinator.generation)
            # make sure the right member_id/generation is set in case they changed
            # while the rejoin was taking place
            self._coordinator.generation = req_generation
            self._coordinator.member_id = req_member_id
            return response.member_assignment

        # Error case
        self._coordinator.request_rejoin()
        if error_type is Errors.RebalanceInProgressError:
            LOG.debug("SyncGroup for group %s failed due to group rebalance", self.group_id)
        elif error_type in (Errors.UnknownMemberIdError, Errors.IllegalGenerationError):
            err = error_type()
            LOG.debug("SyncGroup for group %s failed due to %s,", self.group_id, err)
            self._coordinator.reset_generation()
        elif error_type in (Errors.GroupCoordinatorNotAvailableError, Errors.NotCoordinatorForGroupError):
            err = error_type()
            LOG.debug("SyncGroup for group %s failed due to %s", self.group_id, err)
            self._coordinator.coordinator_dead()
        elif error_type is Errors.GroupAuthorizationFailedError:
            raise error_type(self.group_id)
        else:
            err = error_type()
            LOG.error("Unexpected error from SyncGroup: %s", err)
            raise Errors.KafkaError(repr(err))

        return None
