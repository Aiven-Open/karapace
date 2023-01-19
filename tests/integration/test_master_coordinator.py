"""
karapace - master coordination test

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from contextlib import closing
from karapace.config import set_config_defaults
from karapace.master_coordinator import MasterCoordinator
from tests.integration.utils.kafka_server import KafkaServers
from tests.integration.utils.network import PortRangeInclusive
from tests.utils import new_random_name

import asyncio
import json
import pytest
import requests
import time


def init_admin(config):
    mc = MasterCoordinator(config=config)
    mc.start()
    return mc


def is_master(mc: MasterCoordinator) -> bool:
    """True if `mc` is the master.

    This takes care of a race condition were the flag `master` is set but
    `master_url` is not yet set.
    """
    return bool(mc.sc and mc.sc.are_we_master and mc.sc.master_url)


def has_master(mc: MasterCoordinator) -> bool:
    """True if `mc` has a master."""
    return bool(mc.sc and not mc.sc.are_we_master and mc.sc.master_url)


@pytest.mark.timeout(60)  # Github workflows need a bit of extra time
@pytest.mark.parametrize("strategy", ["lowest", "highest"])
def test_master_selection(port_range: PortRangeInclusive, kafka_servers: KafkaServers, strategy: str) -> None:
    # Use random port to allow for parallel runs.
    with port_range.allocate_port() as port1, port_range.allocate_port() as port2:
        port_aa, port_bb = sorted((port1, port2))
        client_id_aa = new_random_name("master_selection_aa_")
        client_id_bb = new_random_name("master_selection_bb_")
        group_id = new_random_name("group_id")

        config_aa = set_config_defaults(
            {
                "advertised_hostname": "127.0.0.1",
                "bootstrap_uri": kafka_servers.bootstrap_servers,
                "client_id": client_id_aa,
                "group_id": group_id,
                "port": port_aa,
                "master_election_strategy": strategy,
            }
        )
        config_bb = set_config_defaults(
            {
                "advertised_hostname": "127.0.0.1",
                "bootstrap_uri": kafka_servers.bootstrap_servers,
                "client_id": client_id_bb,
                "group_id": group_id,
                "port": port_bb,
                "master_election_strategy": strategy,
            }
        )

        with closing(init_admin(config_aa)) as mc_aa, closing(init_admin(config_bb)) as mc_bb:
            if strategy == "lowest":
                master = mc_aa
                slave = mc_bb
            else:
                master = mc_bb
                slave = mc_aa

            # Wait for the election to happen
            while not is_master(master):
                time.sleep(0.3)

            while not has_master(slave):
                time.sleep(0.3)

            # Make sure the end configuration is as expected
            master_url = f'http://{master.config["host"]}:{master.config["port"]}'
            assert master.sc.election_strategy == strategy
            assert slave.sc.election_strategy == strategy
            assert master.sc.master_url == master_url
            assert slave.sc.master_url == master_url


def test_mixed_eligibility_for_primary_role(kafka_servers: KafkaServers, port_range: PortRangeInclusive) -> None:
    """Test that primary selection works when mixed set of roles is configured for Karapace instances.

    The Kafka group coordinator leader can be any node, it has no relation to Karapace primary role eligibility.
    This tests may select the primary eligible instance to be the group coordinator leader, in this case test does not
    test the scenario fully. The intent is that group leader is from non-primary node.
    """
    client_id = new_random_name("master_selection_")
    group_id = new_random_name("group_id")

    with port_range.allocate_port() as port1, port_range.allocate_port() as port2, port_range.allocate_port() as port3:
        config_primary = set_config_defaults(
            {
                "advertised_hostname": "127.0.0.1",
                "bootstrap_uri": kafka_servers.bootstrap_servers,
                "client_id": client_id,
                "group_id": group_id,
                "port": port1,
                "master_eligibility": True,
            }
        )
        config_non_primary_1 = set_config_defaults(
            {
                "advertised_hostname": "127.0.0.1",
                "bootstrap_uri": kafka_servers.bootstrap_servers,
                "client_id": client_id,
                "group_id": group_id,
                "port": port2,
                "master_eligibility": False,
            }
        )
        config_non_primary_2 = set_config_defaults(
            {
                "advertised_hostname": "127.0.0.1",
                "bootstrap_uri": kafka_servers.bootstrap_servers,
                "client_id": client_id,
                "group_id": group_id,
                "port": port3,
                "master_eligibility": False,
            }
        )

        with closing(init_admin(config_non_primary_1)) as non_primary_1, closing(
            init_admin(config_non_primary_2)
        ) as non_primary_2, closing(init_admin(config_primary)) as primary:

            # Wait for the election to happen
            while not is_master(primary):
                time.sleep(0.3)

            while not has_master(non_primary_1):
                time.sleep(0.3)

            while not has_master(non_primary_2):
                time.sleep(0.3)

            # Make sure the end configuration is as expected
            primary_url = f'http://{primary.config["host"]}:{primary.config["port"]}'
            assert primary.sc.master_url == primary_url
            assert non_primary_1.sc.master_url == primary_url
            assert non_primary_2.sc.master_url == primary_url


def test_no_eligible_master(kafka_servers: KafkaServers, port_range: PortRangeInclusive) -> None:
    client_id = new_random_name("master_selection_")
    group_id = new_random_name("group_id")

    with port_range.allocate_port() as port:
        config_aa = set_config_defaults(
            {
                "advertised_hostname": "127.0.0.1",
                "bootstrap_uri": kafka_servers.bootstrap_servers,
                "client_id": client_id,
                "group_id": group_id,
                "port": port,
                "master_eligibility": False,
            }
        )

        with closing(init_admin(config_aa)) as mc:
            # Wait for the election to happen, ie. flag is not None
            while not mc.sc or mc.sc.are_we_master is None:
                time.sleep(0.3)

            # Make sure the end configuration is as expected
            assert mc.sc.are_we_master is False
            assert mc.sc.master_url is None


async def test_schema_request_forwarding(registry_async_pair):
    master_url, slave_url = registry_async_pair
    max_tries, counter = 5, 0
    wait_time = 0.5
    subject = new_random_name("subject")
    schema = {"type": "string"}
    other_schema = {"type": "int"}
    # Config updates
    for subj_path in [None, subject]:
        if subj_path:
            path = f"config/{subject}"
        else:
            path = "config"
        for compat in ["FULL", "BACKWARD", "FORWARD", "NONE"]:
            resp = requests.put(f"{slave_url}/{path}", json={"compatibility": compat})
            assert resp.ok
            while True:
                if counter >= max_tries:
                    raise Exception("Compat update not propagated")
                resp = requests.get(f"{master_url}/{path}")
                if not resp.ok:
                    print(f"Invalid http status code: {resp.status_code}")
                    continue
                data = resp.json()
                if "compatibilityLevel" not in data:
                    print(f"Invalid response: {data}")
                    counter += 1
                    await asyncio.sleep(wait_time)
                    continue
                if data["compatibilityLevel"] != compat:
                    print(f"Bad compatibility: {data}")
                    counter += 1
                    await asyncio.sleep(wait_time)
                    continue
                break

    # New schema updates, last compatibility is None
    for s in [schema, other_schema]:
        resp = requests.post(f"{slave_url}/subjects/{subject}/versions", json={"schema": json.dumps(s)})
    assert resp.ok
    data = resp.json()
    assert "id" in data, data
    counter = 0
    while True:
        if counter >= max_tries:
            raise Exception("Subject schema data not propagated yet")
        resp = requests.get(f"{master_url}/subjects/{subject}/versions")
        if not resp.ok:
            print(f"Invalid http status code: {resp.status_code}")
            counter += 1
            continue
        data = resp.json()
        if not data:
            print(f"No versions registered for subject {subject} yet")
            counter += 1
            continue
        assert len(data) == 2, data
        assert data[0] == 1, data
        print("Subject schema data propagated")
        break

    # Schema deletions
    resp = requests.delete(f"{slave_url}/subjects/{subject}/versions/1")
    assert resp.ok
    counter = 0
    while True:
        if counter >= max_tries:
            raise Exception("Subject version deletion not propagated yet")
        resp = requests.get(f"{master_url}/subjects/{subject}/versions/1")
        if resp.ok:
            print(f"Subject {subject} still has version 1 on master")
            counter += 1
            continue
        assert resp.status_code == 404
        print(f"Subject {subject} no longer has version 1")
        break

    # Subject deletion
    resp = requests.get(f"{master_url}/subjects/")
    assert resp.ok
    data = resp.json()
    assert subject in data
    resp = requests.delete(f"{slave_url}/subjects/{subject}")
    assert resp.ok
    counter = 0
    while True:
        if counter >= max_tries:
            raise Exception("Subject deletion not propagated yet")
        resp = requests.get(f"{master_url}/subjects/")
        if not resp.ok:
            print("Could not retrieve subject list on master")
            counter += 1
            continue
        data = resp.json()
        assert subject not in data
        break
