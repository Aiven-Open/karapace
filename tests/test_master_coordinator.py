"""
karapace - master coordination test

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from karapace.config import set_config_defaults
from karapace.master_coordinator import MasterCoordinator
import asyncio
import logging

log = logging.getLogger(__name__)

pytest_plugins = "aiohttp.pytest_plugin"

class Timeout(Exception):
    pass


async def init_admin(config):
    mc = MasterCoordinator(config=config)
    asyncio.create_task(mc.run())
    return mc


async def test_master_selection(kafka_server):
    config_aa = set_config_defaults({})
    config_aa["advertised_hostname"] = "127.0.0.1"
    config_aa["bootstrap_uri"] = "127.0.0.1:{}".format(kafka_server["kafka_port"])
    config_aa["client_id"] = "aa"
    config_aa["port"] = 1234

    mc_aa = await init_admin(config_aa)

    config_bb = set_config_defaults({})
    config_bb["advertised_hostname"] = "127.0.0.1"
    config_bb["bootstrap_uri"] = "127.0.0.1:{}".format(kafka_server["kafka_port"])
    config_bb["client_id"] = "bb"
    config_bb["port"] = 5678
    mc_bb = await init_admin(config_bb)
    while True:
        if not (mc_aa.sc or mc_bb.sc):
            log.info("waiting for %r or %r to be initialized", mc_aa.sc, mc_bb.sc)
            await asyncio.sleep(1.0)
            continue
        if not (mc_aa.sc.master or mc_bb.sc.master or mc_aa.sc.master_url or mc_bb.sc.master_url):
            log.info("waiting for %r, %r, %r or %r to be initialized", mc_aa.sc.master, mc_bb.sc.master,
                     mc_aa.sc.master_url, mc_bb.sc.master_url)
            await asyncio.sleep(1.0)
            continue
        assert mc_aa.sc.master is True
        assert mc_bb.sc.master is False
        master_url = "http://{}:{}".format(config_aa["host"], config_aa["port"])
        assert mc_aa.sc.master_url == master_url
        assert mc_bb.sc.master_url == master_url
        break
    mc_aa.close()
    mc_bb.close()
