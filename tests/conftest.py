"""
karapace - conftest

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
import contextlib
import os
import random
import signal
import socket
import subprocess
import time

import pytest

KAFKA_CURRENT_VERSION = "2.1"
BASEDIR = "kafka_2.12-2.1.1"


class Timeout(Exception):
    pass


def port_is_listening(hostname, port, ipv6):
    if ipv6:
        s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM, 0)
    else:
        s = socket.socket()
    s.settimeout(0.5)
    try:
        s.connect((hostname, port))
        return True
    except socket.error:
        return False


def wait_for_port(port, *, hostname="127.0.0.1", wait_time=20.0, ipv6=False):
    start_time = time.monotonic()
    while True:
        if port_is_listening(hostname, port, ipv6):
            break
        elif time.monotonic() - start_time > wait_time:
            raise Timeout("Timeout waiting for port {} on host {}".format(port, hostname))
        time.sleep(2.0)
    print("Port {} on host {} started listening in {} seconds".format(port, hostname, time.monotonic() - start_time))


def get_random_port(*, start=3000, stop=30000, blacklist=None):
    if isinstance(blacklist, int):
        blacklist = [blacklist]

    while True:
        value = random.randrange(start, stop)
        if not blacklist or value not in blacklist:
            return value


@pytest.fixture(scope="session", name="session_tmpdir")
def fixture_session_tmpdir(tmpdir_factory):
    """Create a temporary directory object that's usable in the session scope.  The returned value is a
    function which creates a new temporary directory which will be automatically cleaned up upon exit."""
    tmpdir_obj = tmpdir_factory.mktemp("karapace.sssion.tmpdr.")

    def subdir():
        return tmpdir_obj.mkdtemp(rootdir=tmpdir_obj)

    try:
        yield subdir
    finally:
        with contextlib.suppress(Exception):
            tmpdir_obj.remove(rec=1)


@pytest.fixture(scope="session", name="zkserver")
def fixture_zkserver(session_tmpdir):
    config, proc = zkserver_base(session_tmpdir)
    try:
        yield config
    finally:
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait(timeout=10.0)


@pytest.fixture(scope="session", name="kafka_server")
def fixture_kafka_server(session_tmpdir, zkserver):
    config, proc = kafka_server_base(session_tmpdir, zkserver)
    try:
        yield config
    finally:
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait(timeout=10.0)


def kafka_java_args(heap_mb, kafka_config_path, logs_dir, log4j_properties_path):
    java_args = [
        "-Xmx{}M".format(heap_mb),
        "-Xms{}M".format(heap_mb),
        "-Dkafka.logs.dir={}/logs".format(logs_dir),
        "-Dlog4j.configuration=file:{}".format(log4j_properties_path),
        "-cp",
        ":".join([
            os.path.join(BASEDIR, "libs", "*"),
        ]),
        "kafka.Kafka",
        kafka_config_path,
    ]
    return java_args


def get_java_process_configuration(java_args):
    command = [
        "/usr/bin/java",
        "-server",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=20",
        "-XX:InitiatingHeapOccupancyPercent=35",
        "-XX:+DisableExplicitGC",
        "-XX:+ExitOnOutOfMemoryError",
        "-Djava.awt.headless=true",
        "-Dcom.sun.management.jmxremote",
        "-Dcom.sun.management.jmxremote.authenticate=false",
        "-Dcom.sun.management.jmxremote.ssl=false",
    ]
    command.extend(java_args)
    return command


def kafka_server_base(session_tmpdir, zk):
    datadir = session_tmpdir()
    blacklist = [zk["admin_port"], zk["client_port"]]
    plaintext_port = get_random_port(start=15000, blacklist=blacklist)

    config = {
        "datadir": datadir.join("data").strpath,
        "kafka_keystore_password": "secret",
        "kafka_port": plaintext_port,
        "zookeeper_port": zk["client_port"],
    }

    os.makedirs(config["datadir"])
    advertised_listeners = ",".join([
        f"PLAINTEXT://127.0.0.1:{plaintext_port}",
    ])
    listeners = ",".join([
        f"PLAINTEXT://:{plaintext_port}",
    ])

    kafka_config = {
        "broker.id": 1,
        "broker.rack": "local",
        "advertised.listeners": advertised_listeners,
        "auto.create.topics.enable": True,
        "default.replication.factor": 1,
        "delete.topic.enable": "true",
        "inter.broker.listener.name": "PLAINTEXT",
        "inter.broker.protocol.version": KAFKA_CURRENT_VERSION,
        "listeners": listeners,
        "log.cleaner.enable": "true",
        "log.dirs": config["datadir"],
        "log.message.format.version": KAFKA_CURRENT_VERSION,
        "log.retention.check.interval.ms": 300000,
        "log.segment.bytes": 200 * 1024 * 1024,  # 200 MiB
        "num.io.threads": 8,
        "num.network.threads": 112,
        "num.partitions": 1,
        "num.replica.fetchers": 4,
        "num.recovery.threads.per.data.dir": 1,
        "offsets.topic.replication.factor": 1,
        "socket.receive.buffer.bytes": 100 * 1024,
        "socket.request.max.bytes": 100 * 1024 * 1024,
        "socket.send.buffer.bytes": 100 * 1024,
        "transaction.state.log.min.isr": 1,
        "transaction.state.log.num.partitions": 16,
        "transaction.state.log.replication.factor": 1,
        "zookeeper.connection.timeout.ms": 6000,
        "zookeeper.connect": "{}:{}".format("127.0.0.1", zk["client_port"])
    }
    kafka_config_path = os.path.join(datadir.strpath, "kafka", "config")
    os.makedirs(kafka_config_path)

    kafka_config_path = os.path.join(kafka_config_path, "server.properties")
    with open(kafka_config_path, "w") as fp:
        for key, value in kafka_config.items():
            fp.write(f"{key}={value}\n")

    log4j_properties_path = os.path.join(BASEDIR, "config/log4j.properties")

    kafka_cmd = get_java_process_configuration(
        java_args=kafka_java_args(
            heap_mb=256,
            logs_dir=os.path.join(datadir.strpath, "kafka"),
            log4j_properties_path=log4j_properties_path,
            kafka_config_path=kafka_config_path,
        ),
    )
    proc = subprocess.Popen(kafka_cmd, env={})
    wait_for_port(config["kafka_port"], wait_time=60)
    return config, proc


def zkserver_base(session_tmpdir, subdir="base"):
    datadir = session_tmpdir()
    path = os.path.join(datadir.strpath, subdir)
    os.makedirs(path)
    client_port = get_random_port(start=15000)
    admin_port = get_random_port(start=15000, blacklist=client_port)
    config = {
        "client_port": client_port,
        "admin_port": admin_port,
        "path": path,
    }
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
""".format_map(config)
    zoo_cfg_path = os.path.join(path, "zoo.cfg")
    with open(zoo_cfg_path, "w") as fp:
        fp.write(zoo_cfg)
    env = {
        "CLASSPATH": "/usr/share/java/slf4j/slf4j-simple.jar",
        "ZOO_LOG_DIR": datadir.join("logs").strpath,
    }
    java_args = get_java_process_configuration(
        java_args=[
            "-cp", ":".join([
                os.path.join(BASEDIR, "libs", "*"),
            ]), "org.apache.zookeeper.server.quorum.QuorumPeerMain", zoo_cfg_path
        ]
    )
    proc = subprocess.Popen(java_args, env=env)
    wait_for_port(config["client_port"])
    return config, proc
