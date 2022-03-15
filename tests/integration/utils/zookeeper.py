"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from pathlib import Path
from subprocess import Popen
from tests.integration.utils.config import KafkaDescription, ZKConfig
from tests.integration.utils.process import get_java_process_configuration
from tests.utils import get_random_port, ZK_PORT_RANGE
from typing import List, Tuple


def zk_java_args(cfg_path: Path, kafka_description: KafkaDescription) -> List[str]:
    msg = f"Couldn't find kafka installation at {kafka_description.install_dir} to run integration tests."
    assert kafka_description.install_dir.exists(), msg
    java_args = [
        "-cp",
        str(kafka_description.install_dir / "libs" / "*"),
        "org.apache.zookeeper.server.quorum.QuorumPeerMain",
        str(cfg_path),
    ]
    return java_args


def configure_and_start_zk(zk_dir: Path, kafka_description: KafkaDescription) -> Tuple[ZKConfig, Popen]:
    cfg_path = zk_dir / "zoo.cfg"
    logs_dir = zk_dir / "logs"
    logs_dir.mkdir(parents=True)

    client_port = get_random_port(port_range=ZK_PORT_RANGE, blacklist=[])
    admin_port = get_random_port(port_range=ZK_PORT_RANGE, blacklist=[client_port])
    config = ZKConfig(
        client_port=client_port,
        admin_port=admin_port,
        path=str(zk_dir),
    )
    zoo_cfg = """
# The number of milliseconds of each tick
tickTime=2000
# The number of ticks that the initial
# synchronization phase can take
initLimit=10
# The number of ticks that can pass between
# sending a request and getting an acknowledgement
syncLimit=5
# the directory where the snapshot is stored.
# do not use /tmp for storage, /tmp here is just
# example sakes.
dataDir={path}
# the port at which the clients will connect
clientPort={client_port}
#clientPortAddress=127.0.0.1
# the maximum number of client connections.
# increase this if you need to handle more clients
#maxClientCnxns=60
#
# Be sure to read the maintenance section of the
# administrator guide before turning on autopurge.
#
# http://zookeeper.apache.org/doc/current/zookeeperAdmin.html#sc_maintenance
#
# The number of snapshots to retain in dataDir
#autopurge.snapRetainCount=3
# Purge task interval in hours
# Set to "0" to disable auto purge feature
#autopurge.purgeInterval=1
# admin server
admin.serverPort={admin_port}
admin.enableServer=false
# Allow reconfig calls to be made to add/remove nodes to the cluster on the fly
reconfigEnabled=true
# Don't require authentication for reconfig
skipACL=yes
""".format(
        client_port=config.client_port,
        admin_port=config.admin_port,
        path=config.path,
    )
    cfg_path.write_text(zoo_cfg)
    env = {
        "CLASSPATH": "/usr/share/java/slf4j/slf4j-simple.jar",
        "ZOO_LOG_DIR": str(logs_dir),
    }
    java_args = get_java_process_configuration(
        java_args=zk_java_args(
            cfg_path,
            kafka_description,
        )
    )
    proc = Popen(java_args, env=env)
    return config, proc
