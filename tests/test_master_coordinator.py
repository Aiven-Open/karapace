"""
karapace - master coordination test

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from karapace.config import set_config_defaults
from karapace.master_coordinator import MasterCoordinator

import time


class Timeout(Exception):
    pass


def init_admin(config):
    mc = MasterCoordinator(config=config)
    mc.start()
    return mc


def test_master_selection(kafka_server):
    config_aa = set_config_defaults({})
    config_aa["advertised_hostname"] = "127.0.0.1"
    config_aa["bootstrap_uri"] = "127.0.0.1:{}".format(kafka_server["kafka_port"])
    config_aa["client_id"] = "aa"
    config_aa["port"] = 1234

    mc_aa = init_admin(config_aa)

    config_bb = set_config_defaults({})
    config_bb["advertised_hostname"] = "127.0.0.1"
    config_bb["bootstrap_uri"] = "127.0.0.1:{}".format(kafka_server["kafka_port"])
    config_bb["client_id"] = "bb"
    config_bb["port"] = 5678
    mc_bb = init_admin(config_bb)
    while True:
        if not (mc_aa.sc or mc_bb.sc):
            time.sleep(1.0)
            continue
        if not (mc_aa.sc.master or mc_bb.sc.master or mc_aa.sc.master_url or mc_bb.sc.master_url):
            time.sleep(1.0)
            continue
        assert mc_aa.sc.master is True
        assert mc_bb.sc.master is False
        master_url = "http://{}:{}".format(config_aa["host"], config_aa["port"])
        assert mc_aa.sc.master_url == master_url
        assert mc_bb.sc.master_url == master_url
        break
    mc_aa.close()
    mc_bb.close()
