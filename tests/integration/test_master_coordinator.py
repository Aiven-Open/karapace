"""
karapace - master coordination test

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from karapace.config import set_config_defaults
from karapace.master_coordinator import MasterCoordinator
from tests.utils import KafkaServers, REGISTRY_URI, REST_URI
from typing import Optional

import asyncio
import json
import os
import pytest
import requests
import time


class Timeout(Exception):
    pass


def init_admin(config):
    mc = MasterCoordinator(config=config)
    mc.start()
    return mc


def is_master(mc: MasterCoordinator) -> bool:
    """ True if `mc` is the master.

    This takes care of a race condition were the flag `master` is set but
    `master_url` is not yet set.
    """
    return bool(mc.sc and mc.sc.master and mc.sc.master_url)


def has_master(mc: MasterCoordinator) -> bool:
    """ True if `mc` has a master. """
    return bool(mc.sc and not mc.sc.master and mc.sc.master_url)


@pytest.mark.timeout(60)
@pytest.mark.parametrize("strategy", ["lowest", "highest"])
def test_master_selection(kafka_servers: Optional[KafkaServers], strategy: str) -> None:
    assert kafka_servers, (
        f"test_master_selection can not be used if the env variable `{REGISTRY_URI}` or `{REST_URI}` is set"
    )

    config_aa = set_config_defaults({})
    config_aa["advertised_hostname"] = "127.0.0.1"
    config_aa["bootstrap_uri"] = kafka_servers.bootstrap_servers
    config_aa["client_id"] = "aa"
    config_aa["port"] = 1234
    config_aa["master_election_strategy"] = strategy
    mc_aa = init_admin(config_aa)
    config_bb = set_config_defaults({})
    config_bb["advertised_hostname"] = "127.0.0.1"
    config_bb["bootstrap_uri"] = kafka_servers.bootstrap_servers
    config_bb["client_id"] = "bb"
    config_bb["port"] = 5678
    config_bb["master_election_strategy"] = strategy
    mc_bb = init_admin(config_bb)

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
    mc_aa.close()
    mc_bb.close()


async def test_schema_request_forwarding(registry_async_pair):
    master_url, slave_url = registry_async_pair
    max_tries, counter = 5, 0
    wait_time = 0.5
    subject = os.urandom(16).hex()
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
