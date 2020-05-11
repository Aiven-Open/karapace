#!/bin/bash
set -e
zk_port=${ZK_PORT:-2181}
kafka_port=${KAFKA_PORT:-9092}
kafka_port_alternate=${KAFKA_PORT_ALT:-9093}
kafka_port_internal=${KAFKA_PORT:-29092}
registry_port=${REGISTRY_PORT:-8081}
rest_port=${REST_PORT:-8082}


start_zk(){
  echo "starting zookeeper"
  cat >/opt/zookeeper.properties <<- EOF
dataDir=/var/lib/zookeeper
clientPort=${zk_port}
maxClientCnxns=0
EOF
  mkdir -p /var/lib/zookeeper
  /opt/kafka_2.12-2.4.1/bin/zookeeper-server-start.sh /opt/zookeeper.properties 2>&1 | tee /var/log/zk.log
}

function start_kafka {
  echo "starting kafka"
  if [ "$1" == "single" ]; then
    adv_listeners="PLAINTEXT://kafka:${kafka_port_internal},PLAINTEXT_HOST://localhost:${kafka_port},PLAINTEXT_ALT://${overlay_ip}:${kafka_port_alternate}"
    listeners="PLAINTEXT://0.0.0.0:${kafka_port_internal},PLAINTEXT_HOST://0.0.0.0:${kafka_port},PLAINTEXT_ALT://0.0.0.0:${kafka_port_alternate}"
    zk_host="zk"
  else
    adv_listeners="PLAINTEXT://localhost:${kafka_port}"
    listeners="PLAINTEXT://0.0.0.0:${kafka_port}"
    zk_host="localhost"
  fi
  cat >/opt/server.properties <<- EOF
broker.id=0
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,PLAINTEXT_ALT:PLAINTEXT
advertised.listeners=${adv_listeners}
listeners=${listeners}
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/var/lib/kafka
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=${zk_host}:${zk_port}
zookeeper.connection.timeout.ms=6000
group.initial.rebalance.delay.ms=0
EOF
  mkdir -p /var/lib/kafka
  /opt/wait-for-it.sh "${zk_host}:${zk_port}"
  /opt/kafka_2.12-2.4.1/bin/kafka-server-start.sh /opt/server.properties 2>&1 | tee /var/log/kafka.log
}

start_karapace_registry(){
  echo "starting karapace schema registry"

  if [ "$1" == "single" ]; then
    kafka_host="kafka:${kafka_port_internal}"
    advertised_host="${ADVERTISED_HOST:-registry}"
  else
    kafka_host="127.0.0.1:${kafka_port}"
    advertised_host="127.0.0.1"
  fi
  cat >/opt/karapace.config.json <<- EOF
{
    "advertised_hostname": "${advertised_host}",
    "bootstrap_uri": "${kafka_host}",
    "client_id": "sr-1",
    "compatibility": "FULL",
    "group_id": "schema-registry",
    "host": "0.0.0.0",
    "log_level": "INFO",
    "port": ${registry_port},
    "master_eligibility": true,
    "replication_factor": 1,
    "security_protocol": "PLAINTEXT",
    "ssl_cafile": null,
    "ssl_certfile": null,
    "ssl_keyfile": null,
    "topic_name": "_schemas"
}
EOF
  /opt/wait-for-it.sh "${kafka_host}"
  python3 -m karapace.schema_registry_apis /opt/karapace.config.json 2>&1 | tee /var/log/karapace-registry.log
}

start_karapace_rest(){
  echo "starting karapace rest api"
  if [ "$1" == "single" ]; then
    kafka_host="kafka:${kafka_port_internal}"
    advertised_host="${ADVERTISED_HOST:-rest}"
    registry_host="registry"
  else
    kafka_host="127.0.0.1:${kafka_port}"
    advertised_host="127.0.0.1"
    registry_host="127.0.0.1"
  fi
  # in theory we dont need to advertise the internal hostname since this should always be accessible from the outside
  cat >/opt/karapace_rest.config.json <<- EOF
{
    "advertised_hostname": "${advertised_host}",
    "bootstrap_uri": "${kafka_host}",
    "host": "0.0.0.0",
    "registry_host": "${registry_host}",
    "registry_port": "${registry_port}",
    "log_level": "INFO",
    "port": "${rest_port}",
    "security_protocol": "PLAINTEXT",
    "ssl_cafile": null,
    "ssl_certfile": null,
    "ssl_keyfile": null
}
EOF
  /opt/wait-for-it.sh "${registry_host}:${registry_port}"
  python3 -m karapace.kafka_rest_apis /opt/karapace_rest.config.json 2>&1 | tee /var/log/karapace-rest.log
}

case $1 in
  zk)
    start_zk single &
  ;;
  kafka)
    start_kafka single &
  ;;
  rest)
    start_karapace_rest single &
  ;;
  registry)
    start_karapace_registry single &
  ;;
  all)
    start_zk all &
    start_kafka all &
    # lazy way of figuring out what set of http apps we want
    start_karapace_registry all &
    start_karapace_rest all &
  ;;
  *)
    echo "usage: start-karapace.sh <zk|kafka|registry|rest|all>"
    exit 0
  ;;
esac

wait
