"""
karapace - master coordination test

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import asyncio

import pytest

from karapace.core.config import Config
from karapace.core.coordinator.master_coordinator import MasterCoordinator
from karapace.core.typing import SchemaReaderStoppper
from tests.integration.utils.kafka_server import KafkaServers
from tests.integration.utils.network import allocate_port
from tests.utils import new_random_name


class AlwaysAvailableSchemaReaderStoppper(SchemaReaderStoppper):
    def ready(self) -> bool:
        return True

    def set_not_ready(self) -> None:
        pass


async def init_admin(config):
    mc = MasterCoordinator(config=config)
    mc.set_stoppper(AlwaysAvailableSchemaReaderStoppper())
    mc.start()
    return mc


def is_master(mc: MasterCoordinator) -> bool:
    """True if `mc` is the master.

    This takes care of a race condition were the flag `master` is set but
    `master_url` is not yet set.
    """
    return bool(mc.schema_coordinator and mc.schema_coordinator.are_we_master() and mc.schema_coordinator.master_url)


def has_master(mc: MasterCoordinator) -> bool:
    """True if `mc` has a master."""
    return bool(mc.schema_coordinator and not mc.schema_coordinator.are_we_master() and mc.schema_coordinator.master_url)


@pytest.mark.timeout(65)  # Github workflows need a bit of extra time
@pytest.mark.parametrize("strategy", ["lowest", "highest"])
async def test_master_selection(kafka_servers: KafkaServers, strategy: str) -> None:
    # Use random port to allow for parallel runs.
    with allocate_port() as port1, allocate_port() as port2:
        port_aa, port_bb = sorted((port1, port2))
        client_id_aa = new_random_name("master_selection_aa_")
        client_id_bb = new_random_name("master_selection_bb_")
        group_id = new_random_name("group_id")

        config_aa = Config()
        config_aa.advertised_hostname = "127.0.0.1"
        config_aa.bootstrap_uri = kafka_servers.bootstrap_servers[0]
        config_aa.client_id = client_id_aa
        config_aa.group_id = group_id
        config_aa.port = port_aa
        config_aa.master_election_strategy = strategy

        config_bb = Config()
        config_bb.advertised_hostname = "127.0.0.1"
        config_bb.bootstrap_uri = kafka_servers.bootstrap_servers[0]
        config_bb.client_id = client_id_bb
        config_bb.group_id = group_id
        config_bb.port = port_bb
        config_bb.master_election_strategy = strategy

        mc_aa = await init_admin(config_aa)
        mc_bb = await init_admin(config_bb)
        try:
            if strategy == "lowest":
                master = mc_aa
                slave = mc_bb
            else:
                master = mc_bb
                slave = mc_aa

            # Wait for the election to happen
            while not is_master(master):
                await asyncio.sleep(0.5)

            while not has_master(slave):
                await asyncio.sleep(0.5)

            # Make sure the end configuration is as expected
            master_url = f"http://{master.config.host}:{master.config.port}"
            assert master.schema_coordinator is not None
            assert slave.schema_coordinator is not None
            assert master.schema_coordinator.election_strategy == strategy
            assert slave.schema_coordinator.election_strategy == strategy
            assert master.schema_coordinator.master_url == master_url
            assert slave.schema_coordinator.master_url == master_url
        finally:
            print(f"expected: {master_url}")
            print(slave.schema_coordinator.master_url)
            await mc_aa.close()
            await mc_bb.close()


async def test_mixed_eligibility_for_primary_role(kafka_servers: KafkaServers) -> None:
    """Test that primary selection works when mixed set of roles is configured for Karapace instances.

    The Kafka group coordinator leader can be any node, it has no relation to Karapace primary role eligibility.
    This tests may select the primary eligible instance to be the group coordinator leader, in this case test does not
    test the scenario fully. The intent is that group leader is from non-primary node.
    """
    client_id = new_random_name("master_selection_")
    group_id = new_random_name("group_id")

    with allocate_port() as port1, allocate_port() as port2, allocate_port() as port3:
        config_primary = Config()
        config_primary.advertised_hostname = "127.0.0.1"
        config_primary.bootstrap_uri = kafka_servers.bootstrap_servers[0]
        config_primary.client_id = client_id
        config_primary.group_id = group_id
        config_primary.port = port1
        config_primary.master_eligibility = True

        config_non_primary_1 = Config()
        config_non_primary_1.advertised_hostname = "127.0.0.1"
        config_non_primary_1.bootstrap_uri = kafka_servers.bootstrap_servers[0]
        config_non_primary_1.client_id = client_id
        config_non_primary_1.group_id = group_id
        config_non_primary_1.port = port2
        config_non_primary_1.master_eligibility = False

        config_non_primary_2 = Config()
        config_non_primary_2.advertised_hostname = "127.0.0.1"
        config_non_primary_2.bootstrap_uri = kafka_servers.bootstrap_servers[0]
        config_non_primary_2.client_id = client_id
        config_non_primary_2.group_id = group_id
        config_non_primary_2.port = port3
        config_non_primary_2.master_eligibility = False

        non_primary_1 = await init_admin(config_non_primary_1)
        non_primary_2 = await init_admin(config_non_primary_2)
        primary = await init_admin(config_primary)
        try:
            # Wait for the election to happen
            while not is_master(primary):
                await asyncio.sleep(0.5)

            while not has_master(non_primary_1):
                await asyncio.sleep(0.5)

            while not has_master(non_primary_2):
                await asyncio.sleep(0.5)

            # Make sure the end configuration is as expected
            primary_url = f"http://{primary.config.host}:{primary.config.port}"
            assert primary.schema_coordinator.master_url == primary_url
            assert non_primary_1.schema_coordinator.master_url == primary_url
            assert non_primary_2.schema_coordinator.master_url == primary_url
        finally:
            await non_primary_1.close()
            await non_primary_2.close()
            await primary.close()


async def test_no_eligible_master(kafka_servers: KafkaServers) -> None:
    client_id = new_random_name("master_selection_")
    group_id = new_random_name("group_id")

    with allocate_port() as port:
        config_aa = Config()
        config_aa.advertised_hostname = "127.0.0.1"
        config_aa.bootstrap_uri = kafka_servers.bootstrap_servers[0]
        config_aa.client_id = client_id
        config_aa.group_id = group_id
        config_aa.port = port
        config_aa.master_eligibility = False

        mc = await init_admin(config_aa)
        try:
            # Wait for the election to happen, ie. flag is not None
            while not mc.schema_coordinator or mc.schema_coordinator.are_we_master() is None:
                await asyncio.sleep(0.5)

            # Make sure the end configuration is as expected
            assert mc.schema_coordinator.are_we_master() is False
            assert mc.schema_coordinator.master_url is None
        finally:
            await mc.close()
