"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from pathlib import Path
from subprocess import Popen
from tests.integration.utils.config import KafkaDescription, ZKConfig
from tests.integration.utils.process import get_java_process_configuration
from tests.utils import write_ini


def zk_java_args(cfg_path: Path, kafka_description: KafkaDescription) -> list[str]:
    msg = f"Couldn't find kafka installation at {kafka_description.install_dir} to run integration tests."
    assert kafka_description.install_dir.exists(), msg
    java_args = [
        "-cp",
        str(kafka_description.install_dir / "libs" / "*"),
        "org.apache.zookeeper.server.quorum.QuorumPeerMain",
        str(cfg_path),
    ]
    return java_args


def configure_and_start_zk(config: ZKConfig, kafka_description: KafkaDescription) -> Popen:
    zk_dir = Path(config.path)
    cfg_path = zk_dir / "zoo.cfg"
    logs_dir = zk_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    zoo_cfg = {
        # Number of milliseconds of each tick
        "tickTime": 2000,
        # Number of ticks that the initial synchronization phase can take
        "initLimit": 10,
        # Number of ticks that can pass between sending a request and getting
        # an acknowledgement
        "syncLimit": 5,
        # Directory where the snapshot is stored
        "dataDir": config.path,
        # Port at which the clients will connect
        "clientPort": config.client_port,
        "admin.serverPort": config.admin_port,
        "admin.enableServer": "false",
        # Allow reconfig calls to be made to add/remove nodes to the cluster on the fly
        "reconfigEnabled": "true",
        # Don't require authentication for reconfig
        "skipACL": "yes",
    }

    write_ini(cfg_path, zoo_cfg)
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
    proc = Popen(java_args, env=env)  # pylint: disable=consider-using-with
    return proc
